"""
carga_mp_supabase.py
====================
Sincroniza arquivos CSV da pasta Google Drive "Ministerio_Publico"
para a tabela public."Ministerio_Publico" no Supabase (projeto LUNN).

Fluxo:
  1. Autentica no Google Drive via Service Account
  2. Lista todos os CSV na pasta configurada
  3. Para cada arquivo, extrai mês/ano da coluna DATA
  4. Deleta linhas daquele mês no Supabase (idempotente)
  5. Insere os novos registros em lotes

Dependências:
  pip install google-api-python-client google-auth pandas requests

Variáveis de ambiente (ou .env):
  GOOGLE_SERVICE_ACCOUNT_JSON  -> caminho do JSON da service account
  SUPABASE_URL                 -> https://qhehkgxbpmpptshxlwrb.supabase.co
  SUPABASE_SERVICE_ROLE_KEY    -> chave service_role (sb_secret_... ou JWT)
  DRIVE_FOLDER_ID              -> ID da pasta no Drive (1vr6vSuU9i_nJbd0H9bq4UN9YUZHY2s7N)

Uso:
  python carga_mp_supabase.py              # processa todos os arquivos
  python carga_mp_supabase.py --force      # reprocessa mesmo meses já carregados
"""

import os
import io
import sys
import json
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ─── Configuração ────────────────────────────────────────────────────────────

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://qhehkgxbpmpptshxlwrb.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "1vr6vSuU9i_nJbd0H9bq4UN9YUZHY2s7N")
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")

TABLE_NAME = "Ministerio_Publico"
REST_ENDPOINT = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}"
BATCH_SIZE = 500  # linhas por request POST

# Colunas da tabela Supabase (sem MÊS, ANO, Mês.1 que são derivadas no PBI)
COLUNAS_SUPABASE = [
    "Matricula", "SEXO", "Cargo", "Cargo Agrupado",
    "Lotação", "Tentativa de Regionalização da Lotação",
    "Remuneração Cargo Efetivo1",
    "Outras Verbas Remuneratórias, Legais ou Judiciais2",
    "Função de Confiança ou Cargo em Comissão3",
    "Gratificação Natalina4", "Férias (1/3 Constitucional)5",
    "Abono de Permanência6", "OUTRAS REMUNERAÇÕES TEMPORÁRIAS7",
    "VERBAS INDENIZATÓRIAS8", "Total de Rendimentos Brutos9",
    "Contribuição Previdenciária10", "Imposto de Renda11",
    "Retenção por Teto Constitucional12", "Total de Descontos13",
    "Rendimento Líquido Total14", "DATA",
]

COLUNAS_TEXTO = [
    "Matricula", "SEXO", "Cargo", "Cargo Agrupado",
    "Lotação", "Tentativa de Regionalização da Lotação",
]

COLUNAS_NUMERICAS = [
    "Remuneração Cargo Efetivo1",
    "Outras Verbas Remuneratórias, Legais ou Judiciais2",
    "Função de Confiança ou Cargo em Comissão3",
    "Gratificação Natalina4", "Férias (1/3 Constitucional)5",
    "Abono de Permanência6", "OUTRAS REMUNERAÇÕES TEMPORÁRIAS7",
    "VERBAS INDENIZATÓRIAS8", "Total de Rendimentos Brutos9",
    "Contribuição Previdenciária10", "Imposto de Renda11",
    "Retenção por Teto Constitucional12", "Total de Descontos13",
    "Rendimento Líquido Total14",
]

LOG_FILE = Path(__file__).parent / "carga_mp.log"
PROCESSED_FILE = Path(__file__).parent / "processados_mp.json"

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ─── Google Drive ────────────────────────────────────────────────────────────


def get_drive_service():
    """Autentica e retorna o serviço Google Drive."""
    SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)


def listar_arquivos_csv(service) -> list[dict]:
    """Lista todos os CSV na pasta configurada do Drive."""
    query = (
        f"'{DRIVE_FOLDER_ID}' in parents "
        f"and mimeType='text/csv' "
        f"and trashed=false"
    )
    results = service.files().list(
        q=query,
        fields="files(id, name, modifiedTime)",
        orderBy="modifiedTime desc",
    ).execute()
    return results.get("files", [])


def baixar_arquivo(service, file_id: str) -> io.BytesIO:
    """Baixa um arquivo do Drive para memória."""
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buffer.seek(0)
    return buffer


# ─── Processamento do DataFrame ─────────────────────────────────────────────


def processar_csv(buffer: io.BytesIO) -> pd.DataFrame:
    """Lê o CSV e retorna DataFrame pronto para o Supabase."""
    df = pd.read_csv(buffer, encoding="utf-8", sep=",")

    # Garante que as colunas esperadas existem
    colunas_presentes = [c for c in COLUNAS_SUPABASE if c in df.columns]
    df = df[colunas_presentes].copy()

    # Tipos texto
    for col in COLUNAS_TEXTO:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("nan", None)

    # Tipos numéricos
    for col in COLUNAS_NUMERICAS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # DATA → ISO 8601 com timezone (Supabase espera timestamptz)
    if "DATA" in df.columns:
        df["DATA"] = pd.to_datetime(df["DATA"], errors="coerce")
        df["DATA"] = df["DATA"].dt.tz_localize("America/Sao_Paulo", ambiguous="NaT")
        df["DATA"] = df["DATA"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    # Remove linhas sem DATA (lixo)
    df = df.dropna(subset=["DATA"])

    # Substitui NaN por None para JSON
    df = df.where(pd.notnull(df), None)

    log.info(f"  DataFrame: {len(df)} linhas, {len(df.columns)} colunas")
    return df


def extrair_meses(df: pd.DataFrame) -> list[str]:
    """Retorna lista de meses únicos no formato YYYY-MM."""
    datas = pd.to_datetime(df["DATA"], errors="coerce", utc=True)
    meses = datas.dt.strftime("%Y-%m").dropna().unique().tolist()
    return sorted(meses)


# ─── Supabase ────────────────────────────────────────────────────────────────


def supabase_headers() -> dict:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def deletar_mes(ano_mes: str):
    """Deleta todas as linhas de um mês específico da tabela."""
    ano, mes = ano_mes.split("-")
    inicio = f"{ano}-{mes}-01T00:00:00+00:00"

    # Próximo mês
    m = int(mes)
    a = int(ano)
    if m == 12:
        fim = f"{a+1}-01-01T00:00:00+00:00"
    else:
        fim = f"{a}-{m+1:02d}-01T00:00:00+00:00"

    url = REST_ENDPOINT
    params = {
        "DATA": f"gte.{inicio}",
        "and": f"(DATA.lt.{fim})",
    }
    # A API REST do Supabase para DELETE usa query params como filtro
    # Melhor usar o filtro direto na URL
    delete_url = f"{url}?DATA=gte.{inicio}&DATA=lt.{fim}"

    resp = requests.delete(delete_url, headers=supabase_headers())
    if resp.status_code in (200, 204):
        log.info(f"  Deletadas linhas de {ano_mes}")
    else:
        log.error(f"  Erro ao deletar {ano_mes}: {resp.status_code} - {resp.text}")
        raise RuntimeError(f"DELETE falhou: {resp.status_code}")


def inserir_lote(registros: list[dict]):
    """Insere um lote de registros via POST."""
    resp = requests.post(
        REST_ENDPOINT,
        headers=supabase_headers(),
        json=registros,
    )
    if resp.status_code not in (200, 201):
        log.error(f"  Erro INSERT: {resp.status_code} - {resp.text[:500]}")
        raise RuntimeError(f"INSERT falhou: {resp.status_code}")


def carregar_supabase(df: pd.DataFrame):
    """Deleta meses existentes e insere os dados novos."""
    meses = extrair_meses(df)
    log.info(f"  Meses no arquivo: {meses}")

    # 1) Deletar meses
    for mes in meses:
        deletar_mes(mes)

    # 2) Inserir em lotes
    registros = df.to_dict(orient="records")
    total = len(registros)
    inseridos = 0

    for i in range(0, total, BATCH_SIZE):
        lote = registros[i : i + BATCH_SIZE]
        inserir_lote(lote)
        inseridos += len(lote)
        log.info(f"  Inseridos {inseridos}/{total} ({inseridos*100//total}%)")

    log.info(f"  Carga concluída: {total} linhas inseridas")


# ─── Controle de processados ────────────────────────────────────────────────


def carregar_processados() -> dict:
    if PROCESSED_FILE.exists():
        return json.loads(PROCESSED_FILE.read_text(encoding="utf-8"))
    return {}


def salvar_processados(processados: dict):
    PROCESSED_FILE.write_text(
        json.dumps(processados, indent=2, ensure_ascii=False), encoding="utf-8"
    )


# ─── Main ───────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Carga MP → Supabase")
    parser.add_argument("--force", action="store_true", help="Reprocessa todos")
    args = parser.parse_args()

    if not SUPABASE_KEY:
        log.error("SUPABASE_SERVICE_ROLE_KEY não configurada!")
        sys.exit(1)

    log.info("=" * 60)
    log.info("Início da carga Ministerio_Publico")
    log.info("=" * 60)

    # Google Drive
    service = get_drive_service()
    arquivos = listar_arquivos_csv(service)
    log.info(f"Encontrados {len(arquivos)} arquivo(s) na pasta do Drive")

    if not arquivos:
        log.info("Nenhum arquivo para processar.")
        return

    processados = carregar_processados()

    for arq in arquivos:
        fid = arq["id"]
        nome = arq["name"]
        modified = arq["modifiedTime"]

        # Pula se já processou esta versão (a menos que --force)
        if not args.force and fid in processados:
            if processados[fid].get("modifiedTime") == modified:
                log.info(f"Pulando {nome} (já processado, mesma versão)")
                continue

        log.info(f"Processando: {nome} (id={fid})")

        # Baixar
        buffer = baixar_arquivo(service, fid)
        log.info(f"  Download concluído ({buffer.getbuffer().nbytes / 1024:.0f} KB)")

        # Processar
        df = processar_csv(buffer)

        # Carregar no Supabase
        carregar_supabase(df)

        # Marcar como processado
        processados[fid] = {
            "name": nome,
            "modifiedTime": modified,
            "processedAt": datetime.now(timezone.utc).isoformat(),
            "rows": len(df),
        }
        salvar_processados(processados)

    log.info("Processo finalizado com sucesso!")


if __name__ == "__main__":
    main()
