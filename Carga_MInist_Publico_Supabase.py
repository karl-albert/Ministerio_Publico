""
Carga_MInist_Publico_Supabase.py
================================
Sincroniza CSVs da pasta pública Google Drive "Ministerio_Publico"
para a tabela public."Ministerio_Publico" no Supabase (projeto LUNN).

Fluxo:
  1. Baixa todos os CSVs da pasta pública do Google Drive via gdown
  2. Para cada arquivo, extrai mês/ano da coluna DATA
  3. Deleta linhas daquele mês no Supabase (idempotente)
  4. Insere os novos registros em lotes

Dependências:
  pip install gdown pandas requests

Variáveis de ambiente:
  SUPABASE_URL                 -> https://qhehkgxbpmpptshxlwrb.supabase.co
  SUPABASE_SERVICE_ROLE_KEY    -> chave service_role (sb_secret_...)
  DRIVE_FOLDER_ID              -> ID da pasta no Drive (1vr6vSuU9i_nJbd0H9bq4UN9YUZHY2s7N)

Uso:
  python Carga_MInist_Publico_Supabase.py              # processa arquivos novos
  python Carga_MInist_Publico_Supabase.py --force      # reprocessa todos
"""

import os
import sys
import json
import logging
import argparse
import tempfile
import glob
from datetime import datetime, timezone
from pathlib import Path

import gdown
import pandas as pd
import requests

# ─── Configuração ────────────────────────────────────────────────────────────

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://qhehkgxbpmpptshxlwrb.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "1vr6vSuU9i_nJbd0H9bq4UN9YUZHY2s7N")

TABLE_NAME = "Ministerio_Publico"
REST_ENDPOINT = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}"
BATCH_SIZE = 500

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

# Log só no stdout (GitHub Actions captura automaticamente)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ─── Google Drive (pasta pública via gdown) ──────────────────────────────────


def baixar_pasta_drive(destino: str) -> list[str]:
    """Baixa todos os arquivos da pasta pública do Drive."""
    url = f"https://drive.google.com/drive/folders/{DRIVE_FOLDER_ID}"
    log.info(f"Baixando pasta do Drive: {url}")
    gdown.download_folder(url, output=destino, quiet=False, use_cookies=False)
    # Lista todos os CSVs baixados
    csvs = glob.glob(os.path.join(destino, "**", "*.csv"), recursive=True)
    return sorted(csvs)


# ─── Processamento do DataFrame ─────────────────────────────────────────────


def processar_csv(caminho: str) -> pd.DataFrame:
    """Lê o CSV e retorna DataFrame pronto para o Supabase."""
    df = pd.read_csv(caminho, encoding="utf-8", sep=",")

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


def meses_existentes_supabase() -> set[str]:
    """Consulta quais meses já existem na tabela."""
    url = f"{REST_ENDPOINT}?select=DATA&limit=1&order=DATA.desc"
    # Usa uma query DAX-style pra pegar os meses distintos via RPC ou simples select
    # Simplificado: pega o range de datas
    resp = requests.get(
        f"{REST_ENDPOINT}?select=DATA",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Range-Unit": "items",
            "Range": "0-0",
        },
    )
    return set()  # Retorna vazio; o controle é pelo arquivo processados


def deletar_mes(ano_mes: str):
    """Deleta todas as linhas de um mês específico da tabela."""
    ano, mes = ano_mes.split("-")
    inicio = f"{ano}-{mes}-01T00:00:00-03:00"

    m = int(mes)
    a = int(ano)
    if m == 12:
        fim = f"{a+1}-01-01T00:00:00-03:00"
    else:
        fim = f"{a}-{m+1:02d}-01T00:00:00-03:00"

    delete_url = f"{REST_ENDPOINT}?DATA=gte.{inicio}&DATA=lt.{fim}"

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

    for mes in meses:
        deletar_mes(mes)

    registros = df.to_dict(orient="records")
    total = len(registros)
    inseridos = 0

    for i in range(0, total, BATCH_SIZE):
        lote = registros[i : i + BATCH_SIZE]
        inserir_lote(lote)
        inseridos += len(lote)
        log.info(f"  Inseridos {inseridos}/{total} ({inseridos*100//total}%)")

    log.info(f"  Carga concluída: {total} linhas inseridas")


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

    # Baixa todos os CSVs da pasta pública do Drive
    with tempfile.TemporaryDirectory() as tmpdir:
        csvs = baixar_pasta_drive(tmpdir)
        log.info(f"Encontrados {len(csvs)} CSV(s) na pasta do Drive")

        if not csvs:
            log.info("Nenhum CSV para processar.")
            return

        for caminho in csvs:
            nome = os.path.basename(caminho)
            log.info(f"Processando: {nome}")

            df = processar_csv(caminho)
            carregar_supabase(df)

    log.info("Processo finalizado com sucesso!")


if __name__ == "__main__":
    main()
