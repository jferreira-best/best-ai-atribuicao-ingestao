<#
.SYNOPSIS
    Pipeline RAG (SEDUC) - Modo Azure Blob Direto.
    Fonte: Container 'atribuicao' no Azure Blob Storage.
#>

$ErrorActionPreference = "Stop"

# ==========================================
# 1. CREDENCIAIS E CONFIGURAÇÕES
# ==========================================

# --- [Atenção] Preencha seus dados do Azure Storage aqui ---
$StorageAccountName = "SUA_CHAVE_AQUI"
$StorageAccountKey  = "SUA_CHAVE_AQUI"
$ContainerName      = "atribuicao"   # <-- Seu container existente
$BlobPrefix         = ""             # Deixe vazio "" se os arquivos estiverem na raiz do container

# --- Configurações do Azure AI Search (Indexação) ---
$SearchEndpoint   = "SUA_CHAVE_AQUI" 
$SearchKey        = "SUA_CHAVE_AQUI"
$SearchIndex      = "kb-atribuicao1"


$AoaiEndpoint ="SUA_CHAVE_AQUI"
$AoaiKey ="SUA_CHAVE_AQUI"



$AoaiEmbModel     = "text-embedding-3-large"
$AoaiApiVersion   = "2024-08-01-preview"
$EmbDim           = 3072

# Caminho do arquivo intermediário (será gerado localmente e usado pelo script de ingestão)
$OutputJsonl      = "./dados_processados/kb_atribuicao.jsonl"

# ==========================================
# 2. PREPARAÇÃO DO AMBIENTE
# ==========================================
Write-Host ">>> [1/3] Verificando ambiente Python..." -ForegroundColor Cyan

# Garante que a pasta de saída local exista
$OutDir = Split-Path $OutputJsonl -Parent
if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Force -Path $OutDir | Out-Null }

# Instala dependências se necessário (usa python -m pip para segurança)
if (Test-Path "./requirements.txt") {
    python -m pip install -r requirements.txt | Out-Null
}

# ==========================================
# 3. PROCESSAMENTO (Blob -> JSONL)
# ==========================================
Write-Host "`n>>> [2/3] Processando arquivos do container '$ContainerName'..." -ForegroundColor Cyan

# Monta os argumentos. Note que NÃO usamos --input-dir, forçando o script a ir no Azure.
$prepArgs = @(
    "make_kb_jsonl_atribuicao.py",
    "--account-name", $StorageAccountName,
    "--account-key", $StorageAccountKey,
    "--container", $ContainerName,
    "--output-jsonl", $OutputJsonl,
    "--assunto", "Legislação Educacional",
    "--area-interesse", "Recursos Humanos"
)

# Adiciona prefixo apenas se houver (para evitar erro de argumento vazio)
if (-not [string]::IsNullOrWhiteSpace($BlobPrefix)) {
    $prepArgs += ("--prefix", "$BlobPrefix")
}

# Executa o script Python
python @prepArgs

# Verifica se deu certo
if (-not (Test-Path $OutputJsonl)) {
    Write-Error "ERRO: O JSONL não foi gerado. Verifique:"
    Write-Error "1. Se o Nome da Conta e a Chave do Storage estão corretos."
    Write-Error "2. Se o container '$ContainerName' existe e contém arquivos PDF."
    exit
}

# ==========================================
# 4. INGESTÃO (JSONL -> Azure Search)
# ==========================================
Write-Host "`n>>> [3/3] Enviando dados para o Azure AI Search..." -ForegroundColor Cyan

$ingestArgs = @(
    "ingest_embeddings_azure_search_atribuicao.py",
    "--jsonl-path", $OutputJsonl,
    "--search-endpoint", $SearchEndpoint,
    "--search-api-key", $SearchKey,
    "--search-index", $SearchIndex,
    "--provider", "azure",
    "--aoai-endpoint", $AoaiEndpoint,
    "--aoai-key", $AoaiKey,
    "--aoai-emb-deployment", $AoaiEmbModel,
    "--aoai-api-version", $AoaiApiVersion,
    "--emb-dim", "$EmbDim"
)

python @ingestArgs

Write-Host "`n>>> Processo Finalizado com Sucesso!" -ForegroundColor Green
Write-Host "    Agora você pode iniciar a API com: func start" -ForegroundColor Gray