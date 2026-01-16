#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Ingesta um JSONL no Azure AI Search com embeddings e cria/atualiza o índice
para o cenário de Atribuição (AC/AD) com metadados ricos + Semantic Ranker.

Novidades em relação ao script anterior:
- Campos adicionais (filterable/facetable): norma_tipo, orgao_emissor, data_publicacao,
  ano_letivo, fase_processo, programa, publico_alvo, prazo_inicio, prazo_fim,
  referencias_legais (Collection(Edm.String)).
- Campo 'conhecimento' mantido como filterable/facetable.
- analyzerName="pt.Microsoft" em 'text' e 'doc_title' para melhorar a busca lexical em pt-BR.
- semantic.configurations ("kb-atribuicao-semantic") priorizando doc_title, text e campos-chave.
"""

import os
import json
import time
import argparse
import base64
from datetime import datetime, timezone
from typing import List, Dict, Iterable, Optional

import requests

# dotenv (opcional)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# --------------------------
# Embeddings
# --------------------------
def embed_texts_azure(texts: List[str], endpoint: str, api_key: str, deployment: str,
                      api_version: str = "2024-10-21") -> List[List[float]]:
    """
    Azure OpenAI embeddings. deployment = nome do deployment do modelo de embedding.
    """
    url = f"{endpoint.rstrip('/')}/openai/deployments/{deployment}/embeddings?api-version={api_version}"
    headers = {"api-key": api_key, "Content-Type": "application/json"}
    data = {"input": texts}
    resp = requests.post(url, headers=headers, json=data, timeout=60)
    if resp.status_code >= 300:
        raise RuntimeError(f"Erro Azure Embedding: {resp.status_code} {resp.text}")
    jd = resp.json()
    # Azure retorna data[i].embedding
    return [d["embedding"] for d in jd["data"]]


def embed_texts_openai(texts: List[str], api_key: str, model: str = "text-embedding-3-large") -> List[List[float]]:
    """
    OpenAI público embeddings.
    """
    url = "https://api.openai.com/v1/embeddings"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    data = {"input": texts, "model": model}
    resp = requests.post(url, headers=headers, json=data, timeout=60)
    if resp.status_code >= 300:
        raise RuntimeError(f"Erro OpenAI Embedding: {resp.status_code} {resp.text}")
    jd = resp.json()
    return [d["embedding"] for d in jd["data"]]


def batched(iterable: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]


# --------------------------
# Azure AI Search
# --------------------------
def ensure_index(search_endpoint: str, search_key: str, index_name: str, emb_dim: int,
                 api_version: str = "2023-11-01"):
    import requests
    url = f"{search_endpoint.rstrip('/')}/indexes/{index_name}?api-version={api_version}"
    headers = {"Content-Type": "application/json", "api-key": search_key}

    index_def = {
        "name": index_name,
        "fields": [
            {"name": "id", "type": "Edm.String", "key": True, "searchable": False},

            # CORREÇÃO AQUI: Mudamos de 'pt.Microsoft' para 'pt-BR.microsoft'
            {"name": "text", "type": "Edm.String", "searchable": True, "analyzer": "pt-BR.microsoft"},
            
            {
                "name": "content_vector",
                "type": "Collection(Edm.Single)",
                "searchable": True,
                "dimensions": emb_dim,
                "vectorSearchProfile": "vprofile"
            },

            # Estrutura básica
            {"name": "chunk", "type": "Edm.Int32", "filterable": True, "searchable": False},
            
            # CORREÇÃO AQUI TAMBÉM:
            {"name": "doc_title", "type": "Edm.String", "searchable": True, "analyzer": "pt-BR.microsoft"},
            
            {"name": "source_file", "type": "Edm.String", "filterable": True, "searchable": False},
            {"name": "assunto", "type": "Edm.String", "filterable": True, "facetable": True, "searchable": False},
            {"name": "area_interesse", "type": "Edm.String", "filterable": True, "facetable": True, "searchable": False},

            # Conhecimento (AC/AD)
            {"name": "conhecimento", "type": "Edm.String", "filterable": True, "facetable": True, "searchable": False},

            # Metadados enriquecidos
            {"name": "norma_tipo", "type": "Edm.String", "filterable": True, "facetable": True, "searchable": False},
            {"name": "orgao_emissor", "type": "Edm.String", "filterable": True, "facetable": True, "searchable": False},
            {"name": "data_publicacao", "type": "Edm.DateTimeOffset", "filterable": True, "sortable": True, "searchable": False},
            {"name": "ano_letivo", "type": "Edm.String", "filterable": True, "facetable": True, "searchable": False},
            {"name": "fase_processo", "type": "Edm.String", "filterable": True, "facetable": True, "searchable": False},
            {"name": "programa", "type": "Edm.String", "filterable": True, "facetable": True, "searchable": False},
            {"name": "publico_alvo", "type": "Edm.String", "filterable": True, "facetable": True, "searchable": False},
            {"name": "prazo_inicio", "type": "Edm.DateTimeOffset", "filterable": True, "sortable": True, "searchable": False},
            {"name": "prazo_fim", "type": "Edm.DateTimeOffset", "filterable": True, "sortable": True, "searchable": False},
            {"name": "referencias_legais", "type": "Collection(Edm.String)", "searchable": False, "filterable": True, "facetable": True},

            # Controle
            {"name": "updated_at", "type": "Edm.DateTimeOffset", "filterable": True, "searchable": False},
            {"name": "id_original", "type": "Edm.String", "searchable": False}
        ],

        # >>> Formato compatível com 2023-11-01 (SEM algorithmConfigurations / SEM vectorizers)
        "vectorSearch": {
            "algorithms": [
                {"name": "hnsw", "kind": "hnsw"}
            ],
            "profiles": [
                {"name": "vprofile", "algorithm": "hnsw"}
            ]
        },

        # Semantic Ranker
        # Semantic Ranker (API 2023-11-01 usa PRIORITIZED* em vez de content/keywords)
        "semantic": {
            "configurations": [{
                "name": "kb-atribuicao-semantic",
                "prioritizedFields": {
                    "titleField": {"fieldName": "doc_title"},
                    "prioritizedContentFields": [
                        {"fieldName": "text"}
                    ],
                    "prioritizedKeywordsFields": [
                        {"fieldName": "assunto"},
                        {"fieldName": "area_interesse"},
                        {"fieldName": "conhecimento"},
                        {"fieldName": "norma_tipo"},
                        {"fieldName": "orgao_emissor"},
                        {"fieldName": "ano_letivo"},
                        {"fieldName": "fase_processo"},
                        {"fieldName": "programa"},
                        {"fieldName": "publico_alvo"}
                    ]
                  }
            }]
        }

    }

    resp = requests.put(url, headers=headers, json=index_def, timeout=60)
    if resp.status_code not in (200, 201 , 204):
        raise RuntimeError(f"Falha ao criar/atualizar índice: {resp.status_code} {resp.text}")
    print(f"[ok] Índice criado/atualizado: {index_name} (api-version={api_version})")


def upload_documents(search_endpoint: str, search_key: str, index_name: str, docs: List[Dict],
                     api_version: str):
    url = f"{search_endpoint.rstrip('/')}/indexes/{index_name}/docs/index?api-version={api_version}"
    
    headers = {"Content-Type": "application/json", "api-key": search_key}
    #payload = {"actions": [{"@search.action": "upload", **doc} for doc in docs]}
    payload = {"value": [{"@search.action": "mergeOrUpload", **doc} for doc in docs]}

    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    if resp.status_code >= 300:
        raise RuntimeError(f"Falha ao indexar documentos: {resp.status_code} {resp.text}")


def coerce_dt(dt_str: Optional[str]) -> str:
    if not dt_str:
        return datetime.now(timezone.utc).isoformat()
    try:
        return datetime.fromisoformat(dt_str).astimezone(timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


# --------------------------
# Pipeline
# --------------------------
def load_jsonl(path: str) -> List[Dict]:
    data = []
    with open(path, "r", encoding="utf-8") as r:
        for line in r:
            line = line.strip()
            if not line:
                continue
            try:
                data.append(json.loads(line))
            except Exception:
                pass
    return data


def build_docs(objs: List[Dict], vectors: List[List[float]]) -> List[Dict]:
    docs = []
    for obj, vec in zip(objs, vectors):
        #sid = str(obj.get("id") or obj.get("id_original") or "")
        raw_id = str(obj.get("id") or obj.get("id_original") or "")
        # Codifica o ID para ser seguro (sem caracteres inválidos)
        sid = base64.urlsafe_b64encode(raw_id.encode('utf-8')).decode('ascii').rstrip("=")
        text = str(obj.get("text") or obj.get("content") or "")

        # Mapeia todos os campos esperados pelo índice
        doc = {
            "id": sid,
            "id_original": str(obj.get("id") or ""),
            "text": text,
            "content_vector": vec,
            "chunk": int(obj.get("chunk", 0) or 0),
            "doc_title": str(obj.get("doc_title") or obj.get("title") or ""),
            "source_file": str(obj.get("source_file") or obj.get("source") or ""),
            "assunto": str(obj.get("assunto") or "obras"),
            "area_interesse": str(obj.get("area_interesse") or "conhecimento"),

            # AC/AD
            "conhecimento": str(obj.get("conhecimento") or ""),

            # Metadados enriquecidos
            "norma_tipo": str(obj.get("norma_tipo") or ""),
            "orgao_emissor": str(obj.get("orgao_emissor") or ""),
            "data_publicacao": coerce_dt(obj.get("data_publicacao")) if obj.get("data_publicacao") else None,
            "ano_letivo": str(obj.get("ano_letivo") or ""),
            "fase_processo": str(obj.get("fase_processo") or ""),
            "programa": str(obj.get("programa") or ""),
            "publico_alvo": str(obj.get("publico_alvo") or ""),
            "prazo_inicio": coerce_dt(obj.get("prazo_inicio")) if obj.get("prazo_inicio") else None,
            "prazo_fim": coerce_dt(obj.get("prazo_fim")) if obj.get("prazo_fim") else None,
            "referencias_legais": list(obj.get("referencias_legais") or []),

            # Controle
            "updated_at": coerce_dt(obj.get("updated_at")),
        }

        # Remove chaves com None para evitar falhas de schema
        for k in ["data_publicacao", "prazo_inicio", "prazo_fim"]:
            if doc.get(k) is None:
                del doc[k]

        docs.append(doc)
    return docs


def main():
    ap = argparse.ArgumentParser()
    # JSONL / destino
    ap.add_argument("--jsonl-path", help="Caminho local do JSONL (alternativa a --jsonl-blob, se não usar Storage).")
    ap.add_argument("--jsonl-blob", help="(Opcional) Caminho do JSONL dentro do container (ex.: jsonl/kb.jsonl).")
    ap.add_argument("--container", help="(Opcional) Container do Blob onde está o JSONL.")
    ap.add_argument("--account-name", help="(Opcional) Storage Account Name, se for baixar o JSONL do Blob.")
    ap.add_argument("--account-key", help="(Opcional) Storage Account Key, se for baixar o JSONL do Blob.")

    # Embeddings
    ap.add_argument("--provider", choices=["azure", "openai"], default=os.getenv("PROVIDER", "azure"))
    # Azure OpenAI
    ap.add_argument("--aoai-endpoint", default=os.getenv("AOAI_ENDPOINT"))
    ap.add_argument("--aoai-key", default=os.getenv("AOAI_KEY"))
    ap.add_argument("--aoai-emb-deployment", default=os.getenv("AOAI_EMB_DEPLOYMENT", "text-embedding-3-large"))
    ap.add_argument("--aoai-api-version", default=os.getenv("AOAI_API_VERSION", "2024-10-21"))
    # OpenAI público
    ap.add_argument("--openai-key", default=os.getenv("OPENAI_API_KEY"))
    ap.add_argument("--openai-emb-model", default=os.getenv("OPENAI_EMB_MODEL", "text-embedding-3-large"))

    ap.add_argument("--emb-dim", type=int, default=int(os.getenv("EMB_DIM", "3072")))
    ap.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "16")))

    # Azure Search
    ap.add_argument("--search-endpoint", required=True)
    ap.add_argument("--search-api-key", required=True)
    ap.add_argument("--search-index", required=True)
    ap.add_argument("--search-api-version", default=os.getenv("SEARCH_API_VERSION", "2023-11-01"))

    args = ap.parse_args()

    # 1) obter JSONL (local ou blob)
    jsonl_local_path = None
    if args.jsonl_path:
        jsonl_local_path = args.jsonl_path
    else:
        # baixar do blob se informado
        if not (args.jsonl_blob and args.container and args.account_name and args.account_key):
            raise RuntimeError("Informe --jsonl-path OU (--jsonl-blob + --container + --account-name + --account-key) para baixar do Blob.")
        try:
            from azure.storage.blob import BlobServiceClient
            from azure.core.credentials import AzureNamedKeyCredential
        except Exception:
            raise RuntimeError("Para baixar do Blob, instale azure-storage-blob.")

        cred = AzureNamedKeyCredential(args.account_name, args.account_key)
        bsc = BlobServiceClient(account_url=f"https://{args.account_name}.blob.core.windows.net", credential=cred)
        cc = bsc.get_container_client(args.container)
        if not cc.exists():
            raise RuntimeError(f"Container inexistente: {args.container}")
        tmp_jsonl = f"./tmp_{int(time.time())}.jsonl"
        with open(tmp_jsonl, "wb") as f:
            f.write(cc.download_blob(args.jsonl_blob).readall())
        jsonl_local_path = tmp_jsonl
        print(f"[ok] JSONL baixado do blob: {args.container}/{args.jsonl_blob} -> {tmp_jsonl}")

    objs = load_jsonl(jsonl_local_path)
    if not objs:
        raise RuntimeError("JSONL vazio ou inválido.")

    # 2) criar/atualizar índice
    ensure_index(args.search_endpoint, args.search_api_key, args.search_index, args.emb_dim, api_version=args.search_api_version)

    # 3) gerar embeddings em lotes e indexar
    if args.provider == "azure":
        if not (args.aoai_endpoint and args.aoai_key and args.aoai_emb_deployment):
            raise RuntimeError("Para provider=azure informe --aoai-endpoint, --aoai-key e --aoai-emb-deployment.")
        def make_vecs(texts: List[str]) -> List[List[float]]:
            return embed_texts_azure(texts, args.aoai_endpoint, args.aoai_key, args.aoai_emb_deployment, args.aoai_api_version)
    else:  # openai público
        if not args.openai_key:
            raise RuntimeError("Para provider=openai informe --openai-key.")
        def make_vecs(texts: List[str]) -> List[List[float]]:
            return embed_texts_openai(texts, args.openai_key, args.openai_emb_model)

    B = max(1, int(args.batch_size))
    total = len(objs)
    print(f"[info] Iniciando indexação: {total} registros, batch={B}, emb_dim={args.emb_dim}")

    for start in range(0, total, B):
        slice_objs = objs[start:start+B]
        inputs = [str(o.get("text") or o.get("content") or "") for o in slice_objs]
        # Embeddings
        vecs = make_vecs(inputs)
        if any(len(v) != args.emb_dim for v in vecs):
            raise RuntimeError("Dimensão de embedding retornada não confere com --emb-dim.")
        # Monta docs e envia
        docs = build_docs(slice_objs, vecs)
        upload_documents(args.search_endpoint, args.search_api_key, args.search_index, docs, api_version=args.search_api_version)
        print(f"[ok] Enviado: {start + len(docs)}/{total}")

    print("[done] Ingestão concluída.")


if __name__ == "__main__":
    main()
