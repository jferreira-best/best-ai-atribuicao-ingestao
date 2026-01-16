#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gera um JSONL (chunks) a partir de arquivos locais ou de um container do Azure Blob,
incluindo metadados úteis para busca semântica/filtros no Azure AI Search.

Atualizado para suportar os cenários:
- AC  : Atribuição de Classes
- AD  : Avaliação de Desempenho
- CP  : Confirmação de Participação
- PEI : Programa Ensino Integral

Exemplos:
  # Somente local
  python make_kb_jsonl_atribuicao.py \
    --input-dir ./docs \
    --output-jsonl kb_seduc.jsonl \
    --assunto seduc --area-interesse conhecimento

  # Blob (baixa arquivos e sobe o JSONL de volta)
  python make_kb_jsonl_atribuicao.py \
    --container obras --prefix docs/ \
    --output-jsonl kb_seduc.jsonl \
    --upload-jsonl jsonl/kb_seduc.jsonl \
    --assunto seduc --area-interesse conhecimento \
    --account-name <STORAGE_ACCOUNT> --account-key <STORAGE_KEY>
"""

import os
import re
import json
import argparse
import tempfile
import shutil
from pathlib import Path
from typing import Iterable, List, Tuple, Optional, Dict

# dotenv (opcional)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ====== Azure Blob (opcional) ======
BlobServiceClient = None
AzureNamedKeyCredential = None
try:
    from azure.storage.blob import BlobServiceClient as _BSC
    from azure.core.credentials import AzureNamedKeyCredential as _ANKC
    BlobServiceClient = _BSC
    AzureNamedKeyCredential = _ANKC
except Exception:
    pass

# ====== PDF / DOCX (opcionais) ======
PdfReader = None
try:
    from PyPDF2 import PdfReader as _PdfReader
    PdfReader = _PdfReader
except Exception:
    pass

docx = None
try:
    import docx as _docx
    docx = _docx
except Exception:
    pass


# ==============================
# Utilidades de chunking/IO
# ==============================
def chunk_text(text: str, target_chars: int = 1200, overlap: int = 150) -> Iterable[str]:
    """Chunking simples por tamanho-alvo com overlap."""
    text = (text or "").strip()
    if not text:
        return []
    chunks = []
    i = 0
    N = len(text)
    if target_chars <= 0:
        return [text]
    if overlap < 0:
        overlap = 0
    while i < N:
        j = min(N, i + target_chars)
        chunk = text[i:j]
        chunks.append(chunk.strip())
        if j >= N:
            break
        i = j - overlap if overlap > 0 else j
        if i < 0:
            i = 0
    return [c for c in chunks if c]


def read_txt(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        try:
            return path.read_text(encoding="latin-1", errors="ignore")
        except Exception:
            return ""


def read_pdf(path: Path) -> str:
    if PdfReader is None:
        print(f"[warn] PyPDF2 não instalado; ignorando PDF: {path.name}")
        return ""
    try:
        with open(path, "rb") as f:
            pdf = PdfReader(f)
            parts = []
            for page in pdf.pages:
                txt = (page.extract_text() or "").strip()
                if txt:
                    parts.append(txt)
            return "\n".join(parts)
    except Exception as e:
        print(f"[warn] Falha ao ler PDF {path.name}: {e}")
        return ""


def read_docx(path: Path) -> str:
    if docx is None:
        print(f"[warn] python-docx não instalado; ignorando DOCX: {path.name}")
        return ""
    try:
        d = docx.Document(str(path))
        parts = []
        for p in d.paragraphs:
            t = (p.text or "").strip()
            if t:
                parts.append(t)
        # opcional: tabelas
        for tbl in getattr(d, "tables", []):
            for row in getattr(tbl, "rows", []):
                row_cells = []
                for cell in getattr(row, "cells", []):
                    ct = (cell.text or "").strip()
                    if ct:
                        row_cells.append(ct)
                if row_cells:
                    parts.append(" | ".join(row_cells))
        return "\n".join(parts)
    except Exception as e:
        print(f"[warn] Falha ao ler DOCX {path.name}: {e}")
        return ""


def extract_text_from_file(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in (".txt", ".md", ".csv", ".log"):
        return read_txt(path)
    if suffix == ".pdf":
        return read_pdf(path)
    if suffix == ".docx":
        return read_docx(path)
    # fallback: tenta como texto
    return read_txt(path)


# ==============================
# Heurísticas de metadados
# ==============================
YEAR_RE = re.compile(r"\b(20[2-4]\d)\b")  # 2020–2049 (ajuste se quiser)
DATE_BR_RE = re.compile(r"\b([0-3]?\d)[/.-]([01]?\d)[/.-](20[2-4]\d)\b")  # dd/mm/yyyy ou dd.mm.yyyy

def infer_conhecimento(file_name: str) -> str:
    """
    Define a área de conhecimento baseada no prefixo ou palavras-chave do arquivo.
    Suporta: AC, AD, CP, PEI.
    """
    fn = (file_name or "").strip().upper()
    
    # Mapeamento direto de prefixos
    if fn.startswith("AC"):
        return "Atribuição de Classes (AC)"
    if fn.startswith("AD"):
        return "Avaliação de Desempenho (AD)"
    if fn.startswith("CP"):
        return "Confirmação de Participação (CP)"
    if fn.startswith("PEI"):
        return "Programa Ensino Integral (PEI)"
    
    # Fallback: tentar detectar no meio do nome se não houver prefixo claro
    if "ATRIBUI" in fn: 
        return "Atribuição de Classes (AC)"
    if "AVALIACA" in fn or "DESEMPENHO" in fn: 
        return "Avaliação de Desempenho (AD)"
    if "CONFIRMACAO" in fn and "PARTICIPACAO" in fn:
        return "Confirmação de Participação (CP)"
    if "ENSINO INTEGRAL" in fn:
        return "Programa Ensino Integral (PEI)"
        
    return "Geral"

def norm_str(s: Optional[str]) -> str:
    return (s or "").strip()

def to_iso_date(d: str, m: str, y: str) -> str:
    try:
        dd = int(d); mm = int(m); yy = int(y)
        return f"{yy:04d}-{mm:02d}-{dd:02d}"
    except Exception:
        return ""

def guess_norma_tipo(text: str, fname: str) -> str:
    s = f"{fname}\n{text}".lower()
    if "portaria conjunta" in s: return "Portaria Conjunta"
    if "portaria" in s: return "Portaria"
    if "resolução" in s or "resolucao" in s: return "Resolução"
    if "comunicado" in s: return "Comunicado"
    if "informação" in s or "informacao" in s: return "Informação"
    if "decreto" in s: return "Decreto"
    if "lei complementar" in s: return "Lei Complementar"
    return ""

def guess_orgao_emissor(text: str) -> str:
    s = (text or "").upper()
    # Prioridade para combinações
    if "SUCOR" in s and "SUPED" in s: return "SUCOR/SUPED"
    if "CGRH" in s: return "CGRH"
    for k in ("DIPES", "SUCOR", "SUPED", "SEDUC"):
        if k in s:
            return k
    return ""

def guess_ano_letivo(text: str, fname: str) -> str:
    # prioridade: número que aparece no nome (ex.: 2026)
    ys = re.findall(YEAR_RE, fname or "")
    if ys: return ys[0]
    # senão, do conteúdo (pega o maior ano encontrado, assumindo ser o ano letivo alvo)
    ys = re.findall(YEAR_RE, text or "")
    if ys:
        # converter para int, pegar o max, voltar para str
        try:
            return str(max(map(int, ys)))
        except:
            return ys[0]
    return ""

def guess_fase_processo(text: str, fname: str) -> str:
    """
    Tenta identificar a fase do processo (ex: Inscrição, Alocação, Credenciamento).
    """
    s = f"{fname}\n{text}"
    s_low = s.lower()
    
    # 1. Termos específicos de PEI e CP
    if "conferência de dados" in s_low or "conferencia de dados" in s_low:
        return "Conferência de Dados"
    
    if "credenciamento" in s_low:
        return "Credenciamento"
        
    if "alocação" in s_low or "alocacao" in s_low:
        if "inicial" in s_low:
            return "Alocação Inicial"
        return "Alocação"

    if "realocação" in s_low or "realocacao" in s_low:
        return "Realocação"

    if "transferência" in s_low or "transferencia" in s_low:
        if "pei" in s_low:
            return "Transferência PEI"
        return "Transferência"

    # 2. Confirmação de Participação com fases
    if "confirmação de participação" in s_low or "confirmacao de participacao" in s_low:
        m = re.search(r"fase\s*(\d+)", s_low)
        if m:
            return f"Confirmação de Participação – Fase {m.group(1)}"
        return "Confirmação de Participação"
        
    # 3. Classificação e Inscrição
    if "classificação" in s_low or "classificacao" in s_low:
        return "Classificação"
        
    if "inscrição" in s_low or "inscricao" in s_low:
        return "Inscrição"

    # 4. Avaliação de Desempenho
    if "avaliação de desempenho" in s_low or "avaliacao de desempenho" in s_low:
        if "final" in s_low:
            return "Avaliação de Desempenho Final"
        if "parcial" in s_low:
            return "Avaliação de Desempenho Parcial"
        return "Avaliação de Desempenho"

    return ""

def guess_programa(text: str) -> str:
    s = (text or "").lower()
    # Prioridade alta para PEI
    if "programa ensino integral" in s or " pei " in s or "no pei" in s:
        return "PEI"
    if "ensino integral" in s:
        return "PEI"
        
    if "tempo parcial" in s:
        return "Tempo Parcial"
    if "eja" in s:
        return "EJA"
    if "ensino técnico" in s or "novotec" in s:
        return "Ensino Técnico / Novotec"
        
    return ""

def guess_publico_alvo(text: str) -> str:
    s = (text or "").lower()
    targets = []
    if "docente" in s or "professor" in s:
        targets.append("Docentes")
    if "diretor" in s or "gestor" in s or "coordenador" in s:
        targets.append("Gestores")
    if "candidato" in s or "contratado" in s:
        targets.append("Candidatos/Contratados")
    
    if not targets:
        if "pei" in s: return "PEI"
        return "Geral"
        
    return ", ".join(sorted(list(set(targets))))

def guess_prazos(text: str) -> Tuple[str, str]:
    # procura pares de datas próximas no texto (ex.: "de 13/10 a 31/10/2025")
    # 1) janela “de dd/mm a dd/mm[/yyyy]”
    m = re.search(r"de\s+([0-3]?\d/[01]?\d)(?:/(\d{4}))?\s+a\s+([0-3]?\d/[01]?\d)(?:/(\d{4}))?", text, flags=re.I)
    if m:
        d1, y1, d2, y2 = m.group(1), m.group(2), m.group(3), m.group(4)
        # injeta ano se estiver faltando (tenta achar em volta)
        year_hint = y1 or y2
        if not year_hint:
            around = re.findall(YEAR_RE, text)
            year_hint = around[0] if around else ""
        # quebra dd/mm
        try:
            d1d, d1m = d1.split("/")
            d2d, d2m = d2.split("/")
            ini = to_iso_date(d1d, d1m, year_hint) if year_hint else ""
            fim = to_iso_date(d2d, d2m, year_hint) if year_hint else ""
            return ini, fim
        except Exception:
            pass
    # 2) fallback: primeira e segunda data explícitas dd/mm/yyyy
    ds = re.findall(DATE_BR_RE, text)
    if len(ds) >= 2:
        (d1, m1, y1), (d2, m2, y2) = ds[0], ds[1]
        return to_iso_date(d1, m1, y1), to_iso_date(d2, m2, y2)
    return "", ""

def guess_data_publicacao(text: str) -> str:
    # heurística simples: procura primeira data dd/mm/yyyy; se encontrar, devolve
    m = re.search(DATE_BR_RE, text)
    if m:
        return to_iso_date(m.group(1), m.group(2), m.group(3))
    return ""

def extract_referencias_legais(text: str) -> List[str]:
    refs = set()
    # Resolução SEDUC nº 132/2025; Resolução SE nº 83/2025 ...
    for m in re.finditer(r"(Resolu\w+.*?)\b(\d{1,4})\/(20[1-4]\d)", text, flags=re.I):
        chunk = m.group(0).strip()
        if 6 <= len(chunk) <= 140:
            refs.add(chunk)
    # Portaria nº XXXX/AAAA
    for m in re.finditer(r"(Portaria.*?)\b(\d{1,4})\/(20[1-4]\d)", text, flags=re.I):
        chunk = m.group(0).strip()
        if 6 <= len(chunk) <= 140:
            refs.add(chunk)
    # Lei Complementar
    for m in re.finditer(r"(Lei Complementar.*?)\b(\d{1,4})\/(20\d{2}|19\d{2})", text, flags=re.I):
        chunk = m.group(0).strip()
        if 6 <= len(chunk) <= 140:
            refs.add(chunk)
            
    return sorted(refs)


# ==============================
# Blob helpers
# ==============================
def list_blob_paths(account_name: str, account_key: str, container: str, prefix: Optional[str]) -> List[str]:
    if BlobServiceClient is None or AzureNamedKeyCredential is None:
        raise RuntimeError("azure-storage-blob não está instalado. Instale para listar/baixar do Blob.")
    cred = AzureNamedKeyCredential(account_name, account_key)
    bsc = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=cred)
    cc = bsc.get_container_client(container)
    if not cc.exists():
        raise RuntimeError(f"Container inexistente: {container}")
    items = []
    for blob in cc.list_blobs(name_starts_with=prefix or ""):
        if blob.size and blob.name:
            items.append(blob.name)
    return items


def download_blob_dir(tmp_dir: Path, account_name: str, account_key: str, container: str, prefix: Optional[str]) -> Path:
    if BlobServiceClient is None or AzureNamedKeyCredential is None:
        raise RuntimeError("azure-storage-blob não está instalado. Instale para baixar do Blob.")
    cred = AzureNamedKeyCredential(account_name, account_key)
    bsc = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=cred)
    cc = bsc.get_container_client(container)
    if not cc.exists():
        raise RuntimeError(f"Container inexistente: {container}")

    out_dir = tmp_dir / "input"
    out_dir.mkdir(parents=True, exist_ok=True)

    for blob in cc.list_blobs(name_starts_with=prefix or ""):
        if not blob.size or not blob.name:
            continue
        if not any(blob.name.lower().endswith(ext) for ext in (".pdf", ".docx", ".txt", ".md", ".csv", ".log")):
            continue
        target = out_dir / blob.name
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "wb") as f:
            stream = cc.download_blob(blob.name)
            f.write(stream.readall())
    return out_dir


def upload_jsonl(account_name: str, account_key: str, container: str, blob_path: str, local_jsonl: Path):
    if BlobServiceClient is None or AzureNamedKeyCredential is None:
        raise RuntimeError("azure-storage-blob não está instalado. Instale para enviar ao Blob.")
    cred = AzureNamedKeyCredential(account_name, account_key)
    bsc = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=cred)
    cc = bsc.get_container_client(container)
    if not cc.exists():
        raise RuntimeError(f"Container inexistente: {container}")
    with open(local_jsonl, "rb") as data:
        cc.upload_blob(name=blob_path, data=data, overwrite=True)


# ==============================
# Main
# ==============================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", help="Diretório local com os arquivos para serem convertidos em JSONL.")
    ap.add_argument("--output-jsonl", required=True, help="Caminho local do JSONL de saída (ex.: kb.jsonl).")
    ap.add_argument("--assunto", default=os.getenv("ASSUNTO", "atribuicao"), help="Campo 'assunto' a gravar no JSONL.")
    ap.add_argument("--area-interesse", default=os.getenv("AREA_INTERESSE", "conhecimento"),
                    help="Campo 'area_interesse' a gravar no JSONL.")

    # Azure Blob (opcional)
    ap.add_argument("--account-name", help="Storage Account Name (se usar Blob).")
    ap.add_argument("--account-key", help="Storage Account Key (se usar Blob).")
    ap.add_argument("--container", help="Container do Azure Blob (ex.: obras).")
    ap.add_argument("--prefix", help="Prefixo dentro do container (ex.: docs/).")
    ap.add_argument("--upload-jsonl", help="Se informado, faz upload do JSONL para este caminho no container (ex.: jsonl/kb.jsonl).")

    # Chunking
    ap.add_argument("--target-chars", type=int, default=int(os.getenv("TARGET_CHARS", "1200")))
    ap.add_argument("--overlap", type=int, default=int(os.getenv("OVERLAP", "150")))

    args = ap.parse_args()

    tmp_root = Path(tempfile.mkdtemp(prefix="mkjsonl_"))
    work_dir = None

    try:
        if args.input_dir:
            work_dir = Path(args.input_dir).resolve()
            if not work_dir.exists():
                raise RuntimeError(f"Diretório não existe: {work_dir}")
        else:
            # tenta baixar do blob, se container informado
            if args.container and args.account_name and args.account_key:
                work_dir = download_blob_dir(tmp_root, args.account_name, args.account_key, args.container, args.prefix)
            else:
                raise RuntimeError("Informe --input-dir OU (--container + --account-name + --account-key) para baixar do Blob.")

        # varrer arquivos (mantido do original)
        files = [p for p in work_dir.rglob("*") if p.is_file() and p.suffix.lower() in (".pdf", ".docx", ".txt", ".md", ".csv", ".log")]
        files.sort(key=lambda x: x.name.lower())

        records: List[dict] = []
        for f in files:
            text = extract_text_from_file(f)
            if not text.strip():
                continue

            doc_title = f.stem
            is_gloss = bool(re.search(r"gloss[aá]rio", f.name, flags=re.I))

            # conhecimento por prefixo e nome
            conhecimento = infer_conhecimento(f.name)

            # ------- Metadados enriquecidos (lógica aprimorada) -------
            norma_tipo = guess_norma_tipo(text, f.name)
            orgao_emissor = guess_orgao_emissor(text)
            data_publicacao = guess_data_publicacao(text)
            ano_letivo = guess_ano_letivo(text, f.name)
            fase_processo = guess_fase_processo(text, f.name)
            programa = guess_programa(text)
            publico_alvo = guess_publico_alvo(text)
            prazo_inicio, prazo_fim = guess_prazos(text)
            referencias_legais = extract_referencias_legais(text)
            # -------------------------------------

            for idx, chunk in enumerate(chunk_text(text, target_chars=args.target_chars, overlap=args.overlap), start=1):
                rec: Dict[str, object] = {
                    "id": f"{f.as_posix()}#chunk{idx}",
                    "id_original": f"{f.as_posix()}#chunk{idx}",
                    "source": f.as_posix(),
                    "source_file": f.as_posix(),
                    "doc_title": doc_title,
                    "assunto": args.assunto,
                    "area_interesse": args.area_interesse,
                    "conhecimento": conhecimento,

                    # campos de busca/semântica
                    "content": chunk,
                    "text": chunk,
                    "is_glossario": is_gloss,
                    "chunk": idx,

                    # metadados adicionais (filtros/facetas)
                    "norma_tipo": norma_tipo,
                    "orgao_emissor": orgao_emissor,
                    "data_publicacao": data_publicacao,
                    "ano_letivo": ano_letivo,
                    "fase_processo": fase_processo,
                    "programa": programa,
                    "publico_alvo": publico_alvo,
                    "prazo_inicio": prazo_inicio,
                    "prazo_fim": prazo_fim,
                    "referencias_legais": referencias_legais,
                }
                records.append(rec)

        out = Path(args.output_jsonl).resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", encoding="utf-8") as w:
            for rec in records:
                w.write(json.dumps(rec, ensure_ascii=False) + "\n")

        print(f"[ok] JSONL gerado: {out} (chunks: {len(records)})")

        # Upload opcional
        if args.upload_jsonl:
            if not (args.container and args.account_name and args.account_key):
                raise RuntimeError("Para --upload-jsonl é necessário informar --container, --account-name e --account-key.")
            upload_jsonl(args.account_name, args.account_key, args.container, args.upload_jsonl, out)
            print(f"[ok] JSONL enviado ao blob: {args.container}/{args.upload_jsonl}")

    finally:
        if (not args.input_dir) and tmp_root.exists():
            shutil.rmtree(tmp_root, ignore_errors=True)

    print("[done] Processo concluído.")


if __name__ == "__main__":
    main()