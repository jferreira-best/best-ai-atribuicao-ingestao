import os
import requests
import json
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env (se existir)
load_dotenv()

# --- CONFIGURAÇÕES (Preencha se não usar .env) ---
# Usa as mesmas variáveis que você já configurou
SEARCH_ENDPOINT = os.getenv("SEARCH_ENDPOINT", "https://see-h-ai-crm-searchbot.search.windows.net")
SEARCH_KEY = os.getenv("SEARCH_KEY", "xx")
INDEX_NAME = "kb-atribuicao"  # <--- CONFIRME SE O NOME É ESTE MESMO
API_VERSION = "2023-11-01"

def test_search_index():
    print(f">>> Conectando ao índice: {INDEX_NAME}")
    print(f">>> Endpoint: {SEARCH_ENDPOINT}\n")

    url = f"{SEARCH_ENDPOINT.rstrip('/')}/indexes/{INDEX_NAME}/docs/search?api-version={API_VERSION}"
    headers = {
        "Content-Type": "application/json",
        "api-key": SEARCH_KEY
    }
    
    # Payload para buscar TUDO, retornando apenas o nome do arquivo e o chunk id
    # top=1000 garante que pegamos tudo de uma vez (já que você tem ~253 chunks)
    payload = {
        "search": "*",
        "select": "source_file, chunk, doc_title",
        "top": 1000, 
        "count": True
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        
        total_chunks = data.get("@odata.count", 0)
        results = data.get("value", [])

        # Processar arquivos únicos
        unique_files = {}
        for doc in results:
            fname = doc.get("source_file", "Desconhecido")
            if fname not in unique_files:
                unique_files[fname] = 0
            unique_files[fname] += 1

        print("="*40)
        print(f"RESUMO DO ÍNDICE '{INDEX_NAME}'")
        print("="*40)
        print(f"Total de Chunks (Trechos) encontrados: {total_chunks}")
        print(f"Total de Arquivos Únicos identificados: {len(unique_files)}")
        print("-" * 40)
        print("LISTA DE ARQUIVOS INDEXADOS:")
        
        # Ordena para facilitar a leitura
        for fname in sorted(unique_files.keys()):
            chunks_count = unique_files[fname]
            # Limpa o caminho completo para mostrar só o nome do arquivo, se preferir
            display_name = os.path.basename(fname) 
            print(f"[OK] {display_name} ({chunks_count} chunks)")

        print("="*40)

        if total_chunks == 0:
            print("[ALERTA] O índice existe mas está VAZIO. Verifique se a ingestão rodou mesmo.")

    except Exception as e:
        print(f"[ERRO] Falha ao consultar o índice: {e}")
        # Se der erro 404, o índice não existe com esse nome
        if "404" in str(e):
            print(f"DICA: Verifique se o nome do índice '{INDEX_NAME}' está correto.")

if __name__ == "__main__":
    test_search_index()