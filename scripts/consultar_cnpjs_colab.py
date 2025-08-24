# -*- coding: utf-8 -*-
# ===== IMPORTS =====
import requests
import pandas as pd
import re
import time
import io

# Barra de progresso (Colab/Jupyter-friendly)
try:
    from tqdm.notebook import tqdm
    TQDM = True
except Exception:
    try:
        from tqdm import tqdm
        TQDM = True
    except Exception:
        TQDM = False

# ===== CONFIG =====
COLUNA_CNPJ = "CNPJ"                 # nome da coluna com CNPJs na planilha
SAIDA_EXCEL = "resultado_cnaes.xlsx"
TIMEOUT = 15
SLEEP_ENTRE_CHAMADAS = 0.15
MAX_RETRIES = 3
BACKOFF_BASE = 0.8

# ===== FUNÇÕES AUXILIARES =====
def to_str(x):
    """Converte qualquer coisa em string 'limpa' (sem None/NaN) e ajusta notação científica."""
    if x is None:
        return ""
    s = str(x).strip()
    if re.fullmatch(r"\d+(?:\.\d+)?[eE]\+\d+", s):
        try:
            s = str(int(float(s)))
        except Exception:
            pass
    return s

def apenas_digitos(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def normaliza_cnpj(s: str) -> str:
    return apenas_digitos(to_str(s)).zfill(14)

def cnpj_valido(cnpj: str) -> bool:
    cnpj = apenas_digitos(cnpj)
    if (len(cnpj) != 14) or (cnpj == cnpj[0] * 14):
        return False
    def dv(nums, pesos):
        soma = sum(int(n) * p for n, p in zip(nums, pesos))
        r = soma % 11
        return "0" if r < 2 else str(11 - r)
    p1 = [5,4,3,2,9,8,7,6,5,4,3,2]
    p2 = [6] + p1
    d1 = dv(cnpj[:12], p1)
    d2 = dv(cnpj[:12] + d1, p2)
    return cnpj[-2:] == d1 + d2

def _endereco_formatado(data: dict) -> str:
    """Monta um endereço legível a partir dos campos da API (ignorando vazios)."""
    partes = []
    log = data.get("logradouro", "")
    num = data.get("numero", "")
    comp = data.get("complemento", "")
    bairro = data.get("bairro", "")
    mun = data.get("municipio", "")
    uf = data.get("uf", "")
    cep = data.get("cep", "")
    if log: partes.append(log)
    if num: partes.append(num)
    if comp: partes.append(comp)
    if bairro: partes.append(bairro)
    cidade_uf = " - ".join([p for p in [mun, uf] if p])
    if cidade_uf: partes.append(cidade_uf)
    if cep: partes.append(f"CEP {cep}")
    return ", ".join([p for p in partes if p])

def consulta_brasilapi(cnpj: str) -> dict:
    """
    Consulta a BrasilAPI e retorna APENAS os campos de interesse,
    incluindo 1 CNAE secundário (código + descrição) + endereço, porte,
    capital social e situação cadastral.
    """
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
    tentativas = 0
    while True:
        try:
            r = requests.get(url, timeout=TIMEOUT)
            if r.status_code == 200:
                data = r.json() or {}

                # 1º CNAE secundário (se houver)
                secs = data.get("cnaes_secundarios") or []
                cnae_sec_1 = ""
                cnae_sec_1_desc = ""
                if isinstance(secs, list) and len(secs) > 0 and isinstance(secs[0], dict):
                    cnae_sec_1 = secs[0].get("codigo", "") or ""
                    cnae_sec_1_desc = secs[0].get("descricao", "") or ""

                # Situação cadastral (algumas chaves podem variar entre implementações)
                situacao = data.get("descricao_situacao_cadastral", "") or data.get("situacao_cadastral", "")
                data_sit = data.get("data_situacao_cadastral", "")

                out = {
                    "CNPJ": cnpj,
                    "Razão Social": data.get("razao_social", ""),
                    "Nome Fantasia": data.get("nome_fantasia", ""),
                    "CNAE Principal": data.get("cnae_fiscal", ""),
                    "Descrição CNAE": data.get("cnae_fiscal_descricao", ""),

                    "CNAE Secundário (1º)": cnae_sec_1,
                    "Descrição CNAE Secundário (1º)": cnae_sec_1_desc,

                    "Porte": data.get("porte", ""),
                    "Capital Social": data.get("capital_social", ""),

                    "Situação Cadastral": situacao,
                    "Data Situação Cadastral": data_sit,

                    # Endereço detalhado
                    "Logradouro": data.get("logradouro", ""),
                    "Número": data.get("numero", ""),
                    "Complemento": data.get("complemento", ""),
                    "Bairro": data.get("bairro", ""),
                    "Município": data.get("municipio", ""),
                    "UF": data.get("uf", ""),
                    "CEP": data.get("cep", ""),
                    "Endereço (formatado)": _endereco_formatado(data),

                    # Contatos (se existirem)
                    "E-mail": data.get("email", ""),
                    "Telefone 1": data.get("ddd_telefone_1", ""),
                    "Telefone 2": data.get("ddd_telefone_2", ""),
                }
                return out

            elif r.status_code == 404:
                try:
                    msg = r.json().get("message", "Não encontrado")
                except Exception:
                    msg = "Não encontrado"
                return {
                    "CNPJ": cnpj,
                    "Razão Social": f"Não encontrado: {msg}",
                    "Nome Fantasia": "",
                    "CNAE Principal": "",
                    "Descrição CNAE": "",
                    "CNAE Secundário (1º)": "",
                    "Descrição CNAE Secundário (1º)": "",
                    "Porte": "",
                    "Capital Social": "",
                    "Situação Cadastral": "",
                    "Data Situação Cadastral": "",
                    "Logradouro": "", "Número": "", "Complemento": "",
                    "Bairro": "", "Município": "", "UF": "", "CEP": "",
                    "Endereço (formatado)": "",
                    "E-mail": "", "Telefone 1": "", "Telefone 2": "",
                }

            elif r.status_code in (429, 500, 502, 503, 504):
                tentativas += 1
                if tentativas > MAX_RETRIES:
                    try:
                        msg = r.json().get("message", "")
                    except Exception:
                        msg = f"HTTP {r.status_code}"
                    return {
                        "CNPJ": cnpj,
                        "Razão Social": f"Erro após retries: {msg}",
                        "Nome Fantasia": "",
                        "CNAE Principal": "",
                        "Descrição CNAE": "",
                        "CNAE Secundário (1º)": "",
                        "Descrição CNAE Secundário (1º)": "",
                        "Porte": "",
                        "Capital Social": "",
                        "Situação Cadastral": "",
                        "Data Situação Cadastral": "",
                        "Logradouro": "", "Número": "", "Complemento": "",
                        "Bairro": "", "Município": "", "UF": "", "CEP": "",
                        "Endereço (formatado)": "",
                        "E-mail": "", "Telefone 1": "", "Telefone 2": "",
                    }
                time.sleep(BACKOFF_BASE * (2 ** (tentativas - 1)))
                continue

            else:
                try:
                    msg = r.json().get("message", "")
                except Exception:
                    msg = f"HTTP {r.status_code}"
                return {
                    "CNPJ": cnpj,
                    "Razão Social": f"Erro: {msg}",
                    "Nome Fantasia": "",
                    "CNAE Principal": "",
                    "Descrição CNAE": "",
                    "CNAE Secundário (1º)": "",
                    "Descrição CNAE Secundário (1º)": "",
                    "Porte": "",
                    "Capital Social": "",
                    "Situação Cadastral": "",
                    "Data Situação Cadastral": "",
                    "Logradouro": "", "Número": "", "Complemento": "",
                    "Bairro": "", "Município": "", "UF": "", "CEP": "",
                    "Endereço (formatado)": "",
                    "E-mail": "", "Telefone 1": "", "Telefone 2": "",
                }

        except requests.RequestException as e:
            tentativas += 1
            if tentativas > MAX_RETRIES:
                return {
                    "CNPJ": cnpj,
                    "Razão Social": f"Erro de rede: {e}",
                    "Nome Fantasia": "",
                    "CNAE Principal": "",
                    "Descrição CNAE": "",
                    "CNAE Secundário (1º)": "",
                    "Descrição CNAE Secundário (1º)": "",
                    "Porte": "",
                    "Capital Social": "",
                    "Situação Cadastral": "",
                    "Data Situação Cadastral": "",
                    "Logradouro": "", "Número": "", "Complemento": "",
                    "Bairro": "", "Município": "", "UF": "", "CEP": "",
                    "Endereço (formatado)": "",
                    "E-mail": "", "Telefone 1": "", "Telefone 2": "",
                }
            time.sleep(BACKOFF_BASE * (2 ** (tentativas - 1)))

##Seleção do Arquivc

# ===== UPLOAD DO ARQUIVO (Colab) ANTES DE LER =====
from google.colab import files
print(f"Envie o arquivo Excel (.xlsx/.xls) com a coluna '{COLUNA_CNPJ}'.")
uploaded = files.upload()
if not uploaded:
    raise RuntimeError("Nenhum arquivo enviado.")

nome_arquivo = list(uploaded.keys())[0]
print("Arquivo escolhido:", nome_arquivo)

# Mostrar abas e permitir escolher
xls = pd.ExcelFile(io.BytesIO(uploaded[nome_arquivo]))
print("Abas disponíveis:", xls.sheet_names)
escolha = input("Digite o nome da aba ou deixe vazio para 0 (primeira): ").strip()
sheet = escolha if escolha else 0

# Ler Excel com conversão imediata do CNPJ para string limpa
df = pd.read_excel(
    io.BytesIO(uploaded[nome_arquivo]),
    sheet_name=sheet,
    keep_default_na=False,
    converters={COLUNA_CNPJ: to_str}
)

##PRÉ-PROCESSAMENTO

# ===== PRÉ-PROCESSAMENTO =====
if COLUNA_CNPJ not in df.columns:
    raise KeyError(f"A coluna '{COLUNA_CNPJ}' não foi encontrada na planilha selecionada.")

df[COLUNA_CNPJ] = df[COLUNA_CNPJ].map(normaliza_cnpj)
df["_CNPJ_VALIDO"] = df[COLUNA_CNPJ].map(cnpj_valido)

# ===== CONSULTAS (com deduplicação) =====
lista_todos = df[COLUNA_CNPJ].tolist()
lista_unicos = sorted(set(lista_todos))

resultados_map = {}
iterador = tqdm(lista_unicos, desc="Consultando CNPJs") if TQDM else lista_unicos
ok, inval, err = 0, 0, 0

for cnpj in iterador:
    if not cnpj_valido(cnpj):
        inval += 1
        resultados_map[cnpj] = {
            "CNPJ": cnpj,
            "Razão Social": "CNPJ inválido (DV)",
            "Nome Fantasia": "",
            "CNAE Principal": "",
            "Descrição CNAE": "",
            "CNAE Secundário (1º)": "",
            "Descrição CNAE Secundário (1º)": "",
            "Porte": "",
            "Capital Social": "",
            "Situação Cadastral": "",
            "Data Situação Cadastral": "",
            "Logradouro": "", "Número": "", "Complemento": "",
            "Bairro": "", "Município": "", "UF": "", "CEP": "",
            "Endereço (formatado)": "",
            "E-mail": "", "Telefone 1": "", "Telefone 2": "",
        }
        continue

    res = consulta_brasilapi(cnpj)
    resultados_map[cnpj] = res

    if res["Razão Social"].startswith(("Erro", "Não encontrado")):
        err += 1
    else:
        ok += 1

    time.sleep(SLEEP_ENTRE_CHAMADAS)

# ===== MERGE E SAÍDA (selecionando colunas específicas) =====
df_result = pd.DataFrame([resultados_map[c] for c in lista_unicos])
df_final = df.merge(df_result, how="left", left_on=COLUNA_CNPJ, right_on="CNPJ")

cols_base = [c for c in df.columns if c not in ("_CNPJ_VALIDO",)]
cols_saida = cols_base + [
    "_CNPJ_VALIDO",
    "Razão Social", "Nome Fantasia",
    "CNAE Principal", "Descrição CNAE",
    "CNAE Secundário (1º)", "Descrição CNAE Secundário (1º)",
    "Porte", "Capital Social",
    "Situação Cadastral", "Data Situação Cadastral",
    "Endereço (formatado)",
    "Logradouro", "Número", "Complemento", "Bairro", "Município", "UF", "CEP",
    "E-mail", "Telefone 1", "Telefone 2"
]
df_final = df_final.reindex(columns=cols_saida)

df_final.to_excel(SAIDA_EXCEL, index=False)
print("\nOrigem do arquivo (upload Colab):", nome_arquivo)
print(f"Consulta concluída! Arquivo '{SAIDA_EXCEL}' gerado.")
print(f"Resumo -> Sucesso: {ok} | Inválidos: {inval} | Erros/Não encontrados: {err}")

##Download dos arquivos

#Download dos arquivos

from google.colab import files

files.download(SAIDA_EXCEL)
