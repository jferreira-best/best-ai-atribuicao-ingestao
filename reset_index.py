import os
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
from dotenv import load_dotenv

load_dotenv()

SEARCH_ENDPOINT   = "https://see-h-ai-crm-searchbot.search.windows.net" 
SEARCH_KEY        = "xx"
index_name      = "kb-atribuicao"

endpoint = os.getenv("SEARCH_ENDPOINT")
key = os.getenv("SEARCH_KEY")
index_name = "kb-atribuicao" # Confirme o nome correto

client = SearchIndexClient(endpoint, AzureKeyCredential(key))

try:
    print(f"Apagando índice: {index_name}...")
    client.delete_index(index_name)
    print("Índice apagado com sucesso.")
except Exception as e:
    print(f"Erro ou índice não existia: {e}")