"""
Carga MP Supabase — v2
=======================
Baixa os arquivos de remuneração (.ods e .xlsb) de uma pasta do Google
Drive, aplica os De Paras (Cargo Agrupado, Regionalização da Lotação,
Sexo) e carrega no Supabase.

Essa é a v2 do pipeline: a v1 exigia que o CSV já chegasse pronto/limpo
na pasta do Drive. Essa versão lê os arquivos brutos do MP-SP direto
(mesmo formato que você baixa do portal) e faz a limpeza aqui dentro
antes de gravar — cargo, sexo e lotação mapeados pelo De_Para, e
lotação vazia tratada como "OUTROS Ñ REGIONALIZADOS" em vez de ficar
sem lugar pra ir.

Dependências: pip install -r requirements.txt

Variáveis de ambiente esperadas (Secrets do GitHub Actions):
    SUPABASE_URL              — URL do projeto Supabase
    SUPABASE_SERVICE_ROLE_KEY — chave da API do Supabase
    DRIVE_FOLDER_ID           — ID da pasta do Google Drive com os
                                 arquivos brutos (a mesma de sempre)

Variável opcional:
    CAMINHO_DE_PARA — caminho do De_Para.xlsx (padrão: "de_para/De_Para.xlsx",
                       versionado no próprio repositório)
"""

import pandas as pd
import os
import re
import glob
import sys
import tempfile
import gdown

# ============================================================
# CONFIGURAÇÕES
# ============================================================

CAMINHO_DE_PARA = os.environ.get("CAMINHO_DE_PARA", "de_para/De_Para.xlsx")

# Supabase / Drive — NUNCA hardcode aqui. Vêm de Secrets do GitHub
# Actions e são injetados como variável de ambiente no workflow.
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID")
TABELA = "Ministerio_Publico"

# Texto usado quando a Lotação vem vazia (sem valor algum no arquivo bruto)
LOTACAO_VAZIA_TEXTO = "OUTROS Ñ REGIONALIZADOS"

# ============================================================
# NÃO PRECISA MEXER DAQUI PRA BAIXO
# ============================================================

MESES = {1:"jan",2:"fev",3:"mar",4:"abr",5:"mai",6:"jun",
         7:"jul",8:"ago",9:"set",10:"out",11:"nov",12:"dez"}

COLUNAS_BRUTO = [
    "Matricula","Nome","Cargo","Lotação",
    "Remuneração Cargo Efetivo1",
    "Outras Verbas Remuneratórias, Legais ou Judiciais2",
    "Função de Confiança ou Cargo em Comissão3",
    "Gratificação Natalina4","Férias (1/3 Constitucional)5",
    "Abono de Permanência6","OUTRAS REMUNERAÇÕES TEMPORÁRIAS7",
    "VERBAS INDENIZATÓRIAS8","Total de Rendimentos Brutos9",
    "Contribuição Previdenciária10","Imposto de Renda11",
    "Retenção por Teto Constitucional12","Total de Descontos13",
    "Rendimento Líquido Total14",
]

COLUNAS_SAIDA = [
    "Matricula","SEXO","Cargo","Cargo Agrupado","Lotação",
    "Tentativa de Regionalização da Lotação",
    "Remuneração Cargo Efetivo1",
    "Outras Verbas Remuneratórias, Legais ou Judiciais2",
    "Função de Confiança ou Cargo em Comissão3",
    "Gratificação Natalina4","Férias (1/3 Constitucional)5",
    "Abono de Permanência6","OUTRAS REMUNERAÇÕES TEMPORÁRIAS7",
    "VERBAS INDENIZATÓRIAS8","Total de Rendimentos Brutos9",
    "Contribuição Previdenciária10","Imposto de Renda11",
    "Retenção por Teto Constitucional12","Total de Descontos13",
    "Rendimento Líquido Total14","DATA",
]


def baixar_pasta_drive(destino):
    """Baixa todos os arquivos da pasta do Drive (sem login interativo,
    igual a v1) e devolve só os .ods/.xlsb de remuneração, ordenados."""
    url = f"https://drive.google.com/drive/folders/{DRIVE_FOLDER_ID}"
    print(f"  Baixando pasta do Drive...")
    gdown.download_folder(url, output=destino, quiet=False, use_cookies=False)
    return listar_arquivos(destino)


def listar_arquivos(pasta):
    """Lista todos os .ods e .xlsb na pasta (recursivo), ordenados por nome."""
    arquivos = []
    for ext in ["*.ods", "*.xlsb"]:
        arquivos.extend(glob.glob(os.path.join(pasta, "**", ext), recursive=True))
    # Filtrar apenas arquivos de remuneração
    arquivos = [a for a in arquivos if "servidores-ativos-remuneracao" in os.path.basename(a)]
    return sorted(arquivos)


def extrair_mes_ano(nome_arquivo):
    """Extrai mês e ano do nome do arquivo."""
    match = re.search(r"-(\d{2})-(\d{4})", nome_arquivo)
    if not match:
        raise ValueError(f"Nome fora do padrão: {nome_arquivo}")
    return int(match.group(1)), int(match.group(2))


def carregar_de_paras(caminho):
    """Carrega as 3 tabelas De Para do Excel."""
    df = pd.read_excel(caminho, sheet_name="De_Paras", header=1)

    cargo_map = (
        df[["Cargo","Cargo Agrupado"]]
        .dropna(subset=["Cargo"])
        .drop_duplicates(subset=["Cargo"])
        .set_index("Cargo")["Cargo Agrupado"].to_dict()
    )
    lotacao_map = (
        df[["Lotação","Tentativa de Regionalização da Lotação"]]
        .dropna(subset=["Lotação"])
        .drop_duplicates(subset=["Lotação"])
        .set_index("Lotação")["Tentativa de Regionalização da Lotação"].to_dict()
    )
    nome_map = (
        df[["Nome","Sexo"]]
        .dropna(subset=["Nome"])
        .drop_duplicates(subset=["Nome"])
        .set_index("Nome")["Sexo"].to_dict()
    )
    print(f"  De Paras: {len(cargo_map)} cargos, {len(lotacao_map)} lotações, {len(nome_map)} nomes")
    return cargo_map, lotacao_map, nome_map


def ler_arquivo(caminho):
    """Lê .ods ou .xlsb, detectando automaticamente onde os dados começam."""
    ext = os.path.splitext(caminho)[1].lower()

    if ext == ".xlsb":
        df = pd.read_excel(caminho, engine="pyxlsb", header=None)
    elif ext == ".ods":
        df = pd.read_excel(caminho, engine="odf", header=None, sheet_name="remuneracao")
    else:
        raise ValueError(f"Formato não suportado: {ext}")

    # Detectar onde está o cabeçalho (linha com "Matricula" na coluna 0)
    linha_header = None
    for i in range(min(20, len(df))):
        val = str(df.iloc[i, 0]).strip()
        if val.lower() == "matricula":
            linha_header = i
            break

    if linha_header is None:
        raise ValueError(f"Não encontrei 'Matricula' nas primeiras 20 linhas de {caminho}")

    # Dados começam na linha seguinte ao header
    df = df.iloc[linha_header + 1:]

    # Filtrar apenas linhas com Matricula numérica (remove rodapés e notas)
    df = df[df[0].apply(
        lambda x: str(x).replace(".", "").replace("-", "").isdigit() if pd.notna(x) else False
    )]
    df.columns = COLUNAS_BRUTO
    df = df.reset_index(drop=True)
    return df


def novo_resumo_global():
    """Cria o acumulador de inconsistências usado ao longo do batch todo."""
    return {
        "sem_sexo": 0,
        "sem_cargo": 0,
        "lot_vazia": 0,
        "lot_nao_mapeada": 0,
        "valores_sem_sexo": set(),
        "valores_sem_cargo": set(),
        "valores_lot_nao_mapeada": set(),
    }


def transformar(df, cargo_map, lotacao_map, nome_map, mes, ano, resumo_global):
    """Aplica De Paras, arredonda e adiciona DATA.

    Também acumula em `resumo_global` as inconsistências encontradas
    (contagens por campo + os valores distintos que não bateram com o
    De_Para), para o relatório final do batch.
    """

    df["SEXO"] = df["Nome"].map(nome_map)
    df["Cargo Agrupado"] = df["Cargo"].map(cargo_map)

    # --------------------------------------------------------
    # Lotação: trata separadamente o caso de vir vazia (sem
    # nenhum valor no arquivo bruto) do caso de vir preenchida
    # mas ausente no De_Para.
    # --------------------------------------------------------
    lotacao_str = df["Lotação"].astype(str).str.strip()
    lotacao_vazia = (
        df["Lotação"].isna()
        | (lotacao_str == "")
        | (lotacao_str.str.lower() == "nan")
    )

    df["Tentativa de Regionalização da Lotação"] = df["Lotação"].map(lotacao_map)
    df.loc[lotacao_vazia, "Tentativa de Regionalização da Lotação"] = LOTACAO_VAZIA_TEXTO

    df["DATA"] = f"{mes:02d}/01/{ano} 03:00:00+00"

    # Máscaras de inconsistência (calculadas antes de cortar as colunas,
    # já que "Nome" não sobrevive no COLUNAS_SAIDA)
    sem_sexo_mask = df["SEXO"].isna()
    sem_cargo_mask = df["Cargo Agrupado"].isna()
    # Depois de preencher as vazias, o que ainda for NaN é lotação
    # preenchida mas que não existe no De_Para (precisa mapear).
    lot_nao_mapeada_mask = df["Tentativa de Regionalização da Lotação"].isna() & ~lotacao_vazia

    # Acumula no resumo global
    resumo_global["sem_sexo"] += int(sem_sexo_mask.sum())
    resumo_global["sem_cargo"] += int(sem_cargo_mask.sum())
    resumo_global["lot_vazia"] += int(lotacao_vazia.sum())
    resumo_global["lot_nao_mapeada"] += int(lot_nao_mapeada_mask.sum())
    resumo_global["valores_sem_sexo"].update(df.loc[sem_sexo_mask, "Nome"].dropna().unique())
    resumo_global["valores_sem_cargo"].update(df.loc[sem_cargo_mask, "Cargo"].dropna().unique())
    resumo_global["valores_lot_nao_mapeada"].update(df.loc[lot_nao_mapeada_mask, "Lotação"].dropna().unique())

    df = df[COLUNAS_SAIDA]

    # Arredondar colunas numéricas para 2 casas
    colunas_num = df.select_dtypes(include="number").columns
    df[colunas_num] = df[colunas_num].round(2)

    # Alertas por arquivo
    sem_sexo = int(sem_sexo_mask.sum())
    sem_cargo = int(sem_cargo_mask.sum())
    lot_vazia_count = int(lotacao_vazia.sum())
    lot_nao_mapeada = int(lot_nao_mapeada_mask.sum())
    if sem_sexo: print(f"    ⚠ {sem_sexo} nomes sem SEXO")
    if sem_cargo: print(f"    ⚠ {sem_cargo} cargos sem mapeamento")
    if lot_vazia_count: print(f"    ⚠ {lot_vazia_count} registros com Lotação vazia → marcados como '{LOTACAO_VAZIA_TEXTO}'")
    if lot_nao_mapeada: print(f"    ⚠ {lot_nao_mapeada} lotações preenchidas mas sem mapeamento no De_Para")

    return df


def imprimir_resumo_inconsistencias(resumo_global):
    """Imprime o quadro resumo final + a listinha de valores não mapeados."""

    linhas = [
        ("Nomes sem SEXO no De_Para", resumo_global["sem_sexo"], len(resumo_global["valores_sem_sexo"])),
        ("Cargos sem mapeamento no De_Para", resumo_global["sem_cargo"], len(resumo_global["valores_sem_cargo"])),
        ("Lotações vazias no arquivo (marcadas como 'OUTROS Ñ REGIONALIZADOS')", resumo_global["lot_vazia"], None),
        ("Lotações preenchidas sem mapeamento no De_Para", resumo_global["lot_nao_mapeada"], len(resumo_global["valores_lot_nao_mapeada"])),
    ]

    print(f"\n{'=' * 60}")
    print("QUADRO RESUMO — INCONSISTÊNCIAS")
    print(f"{'=' * 60}")
    largura_campo = max(len(l[0]) for l in linhas)
    cabecalho = f"  {'Campo'.ljust(largura_campo)}   Registros   Valores distintos"
    print(cabecalho)
    print("  " + "-" * (len(cabecalho) - 2))
    for campo, qtd_registros, qtd_distintos in linhas:
        distintos_str = "-" if qtd_distintos is None else str(qtd_distintos)
        print(f"  {campo.ljust(largura_campo)}   {str(qtd_registros).rjust(9)}   {distintos_str.rjust(17)}")

    def listar_valores(titulo, valores):
        valores = sorted(v for v in valores if str(v).strip())
        print(f"\n{titulo} ({len(valores)}):")
        if not valores:
            print("  (nenhum)")
        for v in valores:
            print(f"  - {v}")

    listar_valores("Cargos sem mapeamento no De_Para", resumo_global["valores_sem_cargo"])
    listar_valores("Lotações sem mapeamento no De_Para", resumo_global["valores_lot_nao_mapeada"])
    listar_valores("Nomes sem SEXO mapeado no De_Para", resumo_global["valores_sem_sexo"])


def enviar_supabase(client, df, mes, ano):
    """Deleta mês existente e insere dados novos no Supabase."""

    # Deletar registros do mês
    data_inicio = f"{ano}-{mes:02d}-01"
    if mes == 12:
        data_fim = f"{ano+1}-01-01"
    else:
        data_fim = f"{ano}-{mes+1:02d}-01"

    client.table(TABELA).delete().gte("DATA", data_inicio).lt("DATA", data_fim).execute()
    print(f"    Registros antigos removidos")

    # Substituir NaN por None
    df_upload = df.where(df.notna(), None)

    # Inserir em lotes de 500
    registros = df_upload.to_dict(orient="records")
    for i in range(0, len(registros), 500):
        lote = registros[i:i+500]
        client.table(TABELA).insert(lote).execute()

    print(f"    ✅ {len(registros)} registros inseridos")


def validar_configuracao():
    """Confere que os Secrets/variáveis obrigatórias estão presentes antes
    de gastar tempo baixando/processando qualquer coisa. Falha rápido e
    com mensagem clara — importante rodando sem ninguém olhando (Actions)."""
    faltando = []
    if not SUPABASE_URL:
        faltando.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        faltando.append("SUPABASE_SERVICE_ROLE_KEY")
    if not DRIVE_FOLDER_ID:
        faltando.append("DRIVE_FOLDER_ID")
    if faltando:
        print(f"❌ Variável(is) de ambiente faltando: {', '.join(faltando)}")
        print("   Configure como Secret no GitHub (Settings → Secrets and variables → Actions).")
        sys.exit(1)
    if not os.path.isfile(CAMINHO_DE_PARA):
        print(f"❌ De_Para não encontrado em: {CAMINHO_DE_PARA}")
        sys.exit(1)


def main():
    print("=" * 60)
    print("Carga Ministério Público → Supabase")
    print("=" * 60)

    validar_configuracao()

    # 1. Carregar De Paras (versionado no repo)
    print("\n1. Carregando De Paras...")
    cargo_map, lotacao_map, nome_map = carregar_de_paras(CAMINHO_DE_PARA)

    # 2. Baixar e listar arquivos da pasta do Drive
    print("\n2. Baixando arquivos do Drive...")
    with tempfile.TemporaryDirectory() as tmpdir:
        arquivos = baixar_pasta_drive(tmpdir)
        print(f"\n   Encontrados {len(arquivos)} arquivo(s) de remuneração:")
        for a in arquivos:
            print(f"    → {os.path.basename(a)}")

        if not arquivos:
            print("\nNenhum arquivo novo pra processar. Encerrando sem erro.")
            return

        # 3. Conectar Supabase
        print("\n3. Conectando ao Supabase...")
        from supabase import create_client
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("    Conectado")

        # 4. Processar cada arquivo
        print("\n4. Processando arquivos...\n")
        total_registros = 0
        erros = []
        resumo_global = novo_resumo_global()

        for arquivo in arquivos:
            nome = os.path.basename(arquivo)
            try:
                mes, ano = extrair_mes_ano(nome)
                print(f"  📄 {nome} ({MESES[mes]}/{ano})")

                # Ler
                df = ler_arquivo(arquivo)
                print(f"    Lidos {len(df)} registros")

                # Transformar
                df = transformar(df, cargo_map, lotacao_map, nome_map, mes, ano, resumo_global)

                # Enviar pro Supabase
                enviar_supabase(client, df, mes, ano)
                total_registros += len(df)

            except Exception as e:
                print(f"    ❌ ERRO: {e}")
                erros.append((nome, str(e)))

    # 5. Resumo
    print(f"\n{'=' * 60}")
    print(f"RESUMO")
    print(f"{'=' * 60}")
    print(f"  Arquivos processados: {len(arquivos) - len(erros)}/{len(arquivos)}")
    print(f"  Total de registros: {total_registros}")
    if erros:
        print(f"  Erros ({len(erros)}):")
        for nome, erro in erros:
            print(f"    ❌ {nome}: {erro}")

    # 6. Quadro resumo de inconsistências + valores não mapeados no De_Para
    imprimir_resumo_inconsistencias(resumo_global)

    print(f"\n✅ Concluído!")

    # Sai com código de erro se algum arquivo falhou, pra o GitHub Actions
    # marcar o job como falho (ficar vermelho) em vez de "verdinho" mentindo.
    if erros:
        sys.exit(1)


if __name__ == "__main__":
    main()
    
