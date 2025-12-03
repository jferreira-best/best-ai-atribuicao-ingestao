Aqui está uma proposta completa de **README.md** para o seu projeto. Ele documenta a estrutura, a configuração e o fluxo de dados para os scripts de Atribuição de Classes (AC), Avaliação de Desempenho (AD), Confirmação de Participação (CP) e Programa Ensino Integral (PEI).

-----

# Sistema RAG para Legislação Educacional (SEDUC)

Este projeto implementa um sistema de **RAG (Retrieval-Augmented Generation)** utilizando Python e serviços do Microsoft Azure. O objetivo é permitir buscas semânticas e respostas sintetizadas sobre normativas educacionais (Portarias, Resoluções, Comunicados) referentes a processos como **Atribuição de Classes**, **Avaliação de Desempenho** e **Programa Ensino Integral (PEI)**.

## 📋 Visão Geral da Arquitetura

O sistema é composto por três etapas principais:

1.  **Preparação de Dados (`make_kb_jsonl_atribuicao.py`)**:

      * Lê arquivos (PDF, DOCX, TXT) locais ou do Azure Blob Storage.
      * Realiza a quebra de texto (*chunking*) e extração de metadados ricos (Tipo de Norma, Fase do Processo, Ano Letivo, etc.).
      * Categoriza automaticamente os documentos em: **AC** (Atribuição), **AD** (Avaliação de Desempenho), **CP** (Confirmação de Participação) e **PEI**.
      * Gera um arquivo `.jsonl`.

2.  **Ingestão e Indexação (`ingest_embeddings_azure_search_atribuicao.py`)**:

      * Lê o arquivo `.jsonl`.
      * Gera *embeddings* (vetores) para cada trecho de texto usando Azure OpenAI ou OpenAI público.
      * Cria/Atualiza o índice no **Azure AI Search** com suporte a busca vetorial (HNSW) e *Semantic Ranker*.

3.  **API de Busca (`function_app.py`)**:

      * Uma **Azure Function** que recebe perguntas do usuário.
      * Realiza busca híbrida (Vetorial + Palavras-chave) com reclassificação semântica.
      * Utiliza um LLM (GPT-4o/GPT-3.5) para sintetizar uma resposta baseada estritamente nos documentos encontrados, com citações.

-----

## 🚀 Pré-requisitos

  * Python 3.9+
  * Conta no Azure com os serviços:
      * **Azure AI Search** (com Semantic Search habilitado).
      * **Azure OpenAI** (para Embeddings e Chat) OU chave da OpenAI pública.
      * **Azure Blob Storage** (opcional, para leitura de arquivos).
  * Bibliotecas Python (veja `requirements.txt` sugerido abaixo).

### Instalação das Dependências

```bash
pip install azure-functions azure-search-documents azure-storage-blob pypdf python-docx requests python-dotenv
```

-----

## ⚙️ Configuração (Variáveis de Ambiente)

Crie um arquivo `.env` ou configure as variáveis no seu ambiente de implantação (Azure Functions App Settings).

### Azure AI Search

| Variável | Descrição |
| :--- | :--- |
| `SEARCH_ENDPOINT` | URL do serviço de busca (ex: `https://meu-search.search.windows.net`) |
| `SEARCH_API_KEY` | Chave de administração do Search |
| `SEARCH_INDEX` | Nome do índice (ex: `kb-atribuicao`) |

### LLM e Embeddings (Azure OpenAI)

| Variável | Descrição |
| :--- | :--- |
| `AOAI_ENDPOINT` | Endpoint do Azure OpenAI |
| `AOAI_API_KEY` | Chave do Azure OpenAI |
| `AOAI_EMB_DEPLOYMENT` | Nome do deployment de embedding (ex: `text-embedding-3-large`) |
| `AOAI_CHAT_DEPLOYMENT`| Nome do deployment de chat (ex: `gpt-4o`) |
| `AOAI_API_VERSION` | Versão da API (ex: `2024-02-15-preview`) |

### Configurações Gerais

| Variável | Descrição |
| :--- | :--- |
| `ENABLE_SEMANTIC` | `true` para ativar o Semantic Ranker no Search. |
| `EMBED_DIM` | Dimensão do vetor (ex: `3072` para *text-embedding-3-large*). |
| `HTTP_TIMEOUT_LONG` | Timeout para chamadas ao LLM (ex: `30`). |

-----

## 🛠️ Como Executar

### 1\. Preparação dos Dados (`make_kb_jsonl_atribuicao.py`)

Este script converte seus PDFs em um formato JSONL enriquecido. Ele reconhece automaticamente prefixos como **AC**, **AD**, **CP** e **PEI**.

**Exemplo Local:**

```bash
python make_kb_jsonl_atribuicao.py \
  --input-dir ./meus_documentos_pdf \
  --output-jsonl ./dados_processados/kb_atribuicao.jsonl \
  --assunto "Legislação Educacional"
```

**Exemplo via Azure Blob:**

```bash
python make_kb_jsonl_atribuicao.py \
  --container "documentos-rh" \
  --prefix "normativas/" \
  --output-jsonl kb_atribuicao.jsonl \
  --upload-jsonl "processados/kb_atribuicao.jsonl" \
  --account-name "meustorage" \
  --account-key "MINHA_KEY"
```

### 2\. Ingestão e Indexação (`ingest_embeddings_azure_search_atribuicao.py`)

Este script gera os vetores e envia tudo para o Azure AI Search.

```bash
python ingest_embeddings_azure_search_atribuicao.py \
  --jsonl-path ./dados_processados/kb_atribuicao.jsonl \
  --search-endpoint "https://meu-search.search.windows.net" \
  --search-api-key "MINHA_SEARCH_KEY" \
  --search-index "kb-atribuicao-v1" \
  --provider azure \
  --aoai-endpoint "https://meu-openai.openai.azure.com" \
  --aoai-key "MINHA_AOAI_KEY" \
  --aoai-emb-deployment "text-embedding-3-large"
```

### 3\. Rodando a API (`function_app.py`)

Para rodar localmente com o Azure Functions Core Tools:

```bash
func start
```

**Endpoint de Teste:**
`POST http://localhost:7071/api/search`

**Corpo da Requisição (JSON):**

```json
{
  "query": "Como funciona o cálculo da nota final na Avaliação de Desempenho?",
  "topK": 5,
  "debug": false
}
```

-----

## 📂 Estrutura de Arquivos e Metadados

O sistema foi otimizado para lidar com diferentes tipos de documentos legislativos. O script de preparação (`make_kb_jsonl`) extrai automaticamente os seguintes campos para melhorar a filtragem e a resposta do LLM:

  * **`conhecimento`**: Categoriza o documento macro.
      * *Atribuição de Classes (AC)*
      * *Avaliação de Desempenho (AD)*
      * *Confirmação de Participação (CP)*
      * *Programa Ensino Integral (PEI)*
  * **`fase_processo`**: Identifica sub-etapas (ex: "Credenciamento", "Alocação", "Conferência de Dados").
  * **`norma_tipo`**: Portaria, Resolução, Comunicado, Decreto.
  * **`ano_letivo`**: Ano de vigência detectado no texto.
  * **`referencias_legais`**: Lista de outras leis citadas no texto.

-----

## 🧠 Detalhes do Funcionamento da API

1.  **Detecção de Intenção**: Identifica se a pergunta é do tipo "definição curta" ou complexa.
2.  **Busca Paralela**: Executa busca vetorial (similaridade semântica) e busca textual (palavras-chave) simultaneamente.
3.  **Co-ocorrência**: Implementa lógica para trazer trechos que contêm termos da pergunta próximos uns dos outros, mesmo que o score vetorial seja baixo.
4.  **Prompt Engineering**: O prompt do sistema instrui o LLM a:
      * Responder em PT-BR.
      * Ser objetivo (1 parágrafo).
      * Citar as fontes (ex: `[fonte: Portaria 123]`).
      * Identificar lacunas de informação ("Quer mais detalhes?").

-----

## ⚠️ Notas Importantes

  * **Tabelas em PDFs**: O script usa `PyPDF2`. Para documentos complexos como os de Avaliação de Desempenho (AD) que contêm muitas tabelas (ex: Matriz de Avaliadores), o texto pode ser extraído de forma linear. O LLM geralmente consegue interpretar, mas bibliotecas como `pdfplumber` podem ser integradas no script de preparação para melhores resultados.
  * **Cache**: O sistema possui um cache local (`LRU`) para embeddings de perguntas repetidas, economizando custos de API.