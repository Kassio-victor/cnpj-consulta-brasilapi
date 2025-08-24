# Consulta de CNPJs em lote (Python + BrasilAPI)

Script para consultar **diversos CNPJs** de uma vez no **Google Colab** usando a **BrasilAPI**.
Entrada: planilha com a coluna `CNPJ`.  
Saída: `resultado_cnaes.xlsx` com Razão Social, Nome Fantasia, CNAE principal e 1 secundário (com descrições), endereço, porte, capital social, situação cadastral (com data) e contatos.

## Como usar (Colab)
1. Abra um notebook no Google Colab.
2. Copie e cole o conteúdo de `scripts/consultar_cnpjs_colab.py` em uma célula.
3. Rode a célula e faça upload do Excel quando solicitado.
4. Baixe o arquivo `resultado_cnaes.xlsx` no final.

## Observações
- Dados públicos de **Pessoa Jurídica** via **BrasilAPI**.
- O script valida CNPJ, deduplica e usa retries/backoff para estabilidade.
- Evite expor CNPJs reais em prints públicos.
