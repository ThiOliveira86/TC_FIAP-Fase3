# =====================================================================
# Tech Challenge Fase 3 - Etapa 4 - versao 2
# Job: Silver por edicao  ->  Silver unificada
#
# Mudancas em relacao a versao 1:
#   1. passou de 9 para 30+ colunas, cobrindo as 7 perguntas do desafio
#   2. as colunas nao sao mais localizadas por nome fixo, e sim por
#      padrao de texto (regex). Isso resolve de vez o problema da letra
#      que muda de lugar entre as edicoes (2_r vira 2_q)
#   3. imprime no log qual coluna original virou qual coluna canonica,
#      para conferencia manual
#   4. gera um relatorio de cobertura por ano, porque cinco colunas da
#      pesquisa tem mais de 80 por cento de nulo e isso precisa estar
#      documentado na apresentacao
#   5. cria colunas booleanas a partir dos campos de multipla escolha
#      (linguagens, banco, cloud, bi) para responder a Pergunta 4
#
# Nomenclatura: mantida a do projeto original (senioridade,
# forma_trabalho). As colunas novas seguem o dicionario do grupo.
# =====================================================================

import re
import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

BUCKET = "tc-fase3-state-of-data-seu-id"
ORIGEM = f"s3://{BUCKET}/silver/state_of_data"
DESTINO = f"s3://{BUCKET}/silver_final/state_of_data_unificado"
DESTINO_COBERTURA = f"s3://{BUCKET}/silver_final/relatorio_cobertura"

EDICOES = ["2023_2024", "2024_2025", "2025_2026"]

# =====================================================================
# 1. LOCALIZACAO DAS COLUNAS POR PADRAO
#
# Cada item e uma dupla: (nome_canonico, lista de regex).
# O job percorre os regex na ordem e fica com o primeiro que casar.
# Se mais de uma coluna casar com o mesmo regex, ele escolhe a de nome
# mais curto, que costuma ser a pergunta principal e nao um desdobramento.
#
# A ORDEM DESTA LISTA IMPORTA. Os nomes mais especificos vem primeiro,
# porque a coluna ja usada sai do bolo e nao pode ser escolhida de novo.
# Exemplo: forma_trabalho_ideal precisa ser resolvida antes de
# forma_trabalho, senao a segunda rouba a coluna da primeira.
# =====================================================================
PADROES = [
    # ---- bloco 1: perfil demografico -------------------------------
    ("faixa_idade",           [r"faixa_idade$", r"faixa_de_idade$"]),
    ("idade",                 [r"_a_idade$", r"(^|_)idade$"]),
    ("genero",                [r"_g[e_]nero$", r"(^|_)genero$"]),
    ("cor_raca",              [r"cor_ra[c_]a", r"etnia"]),
    ("pcd",                   [r"(^|_)pcd$", r"pessoa_com_defici"]),
    ("uf",                    [r"uf_onde_mora$", r"(^|_)uf$", r"estado_onde_mora$"]),
    ("regiao",                [r"regi[a_]o_onde_mora$", r"(^|_)regiao$"]),
    ("nivel_ensino",          [r"n[i_]vel_de_ensino$", r"nivel_ensino$"]),
    # ATENCAO: o Glue troca acento por underline. "Area de Formacao" com
    # acento vira "_rea_de_forma_o". Por isso os padroes daqui em diante
    # nunca escrevem a letra acentuada, usam ponto ou param antes dela.
    ("area_formacao",         [r"rea_de_forma", r"area_formacao$"]),

    # ---- bloco 2: situacao profissional ----------------------------
    # 2023: p2_a_qual_sua_situa_o_atual_de_trabalho
    # 2024 e 2025: 2_a_situa_o_de_trabalho
    ("situacao_trabalho",     [r"situa.*_de_trabalho", r"situacao_trabalho$"]),
    ("num_funcionarios",      [r"n[u_]mero_de_funcion", r"num_funcionarios$",
                               r"quantidade_de_funcion"]),
    ("setor",                 [r"setor_de_atua", r"(^|_)setor$"]),
    ("cargo_gestor",          [r"cargo_como_gestor$", r"cargo_gestor$"]),
    ("cargo",                 [r"cargo_atual$", r"(^|_)cargo$"]),
    ("senioridade",           [r"_g_n[i_]vel$", r"(^|_)n[i_]vel$"]),
    ("faixa_salarial",        [r"faixa_salarial$", r"faixa_salarial_mensal$"]),
    # exp_ti vem antes de exp_dados de proposito: o nome da coluna de TI
    # contem "rea_de_dados" no final ("antes de migrar para a area de
    # dados"), entao se dados resolvesse primeiro roubaria a coluna errada
    ("exp_ti",                [r"tempo_de_experiencia_em_ti$", r"experi.*rea_de_ti",
                               r"exp_ti$"]),
    ("exp_dados",             [r"tempo_de_experiencia_em_dados$", r"experi.*rea_de_dados",
                               r"exp_dados$"]),
    ("num_pessoas_dados",     [r"numero_de_pessoas_em_dados$",
                               r"pessoas_que_atuam_com_dados",
                               r"n.mero_aproximado_de_pessoas",
                               r"quantas_pessoas.*dados", r"num_pessoas_dados$"]),
    ("cargos_time",           [r"cargos_no_time_de_dados", r"cargos_fazem_parte_do_time",
                               r"cargos.*time.*dados", r"cargos_time$"]),
    ("funcao_atuacao",        [r"funcao_de_atuacao$", r"_1_atuacao",
                               r"(^|_)atuacao$", r"atua.*reflete"]),

    # ---- bloco 3: tecnologias --------------------------------------
    # atencao: a pergunta usa "linguagens" no plural, entao o padrao
    # precisa parar em "lingua" para casar com singular e plural
    ("linguagem_preferida",   [r"linguagem_preferida$", r"lingua.*preferida"]),
    # 2023: p4_d_quais_das_linguagens_listadas_abaixo_voc_utiliza_no_trabalho
    # 2024: 4_d_linguagem_de_programacao_dia_a_dia
    # 2025: nao existe coluna unica, a pergunta virou uma coluna por
    #       linguagem (4_c_1_sql, 4_c_2_r, 4_c_3_python). Fica nulo nesse
    #       ano e as flags sao preenchidas pelo bloco FLAGS_DUMMY.
    ("linguagens",            [r"linguagem_de_programacao_dia_a_dia",
                               r"linguagens.*utiliza", r"(^|_)linguagens$"]),
    ("bi_preferida",          [r"ferramenta_de_bi_preferida", r"bi.*preferida",
                               r"bi_preferida$"]),
    # NAO usar r"(^|_)bi$" aqui. Na edicao 2023 isso casava com
    # p3_b_6_analista_de_business_intelligence_bi, que e um checkbox de
    # composicao do time, nao a pergunta de ferramentas usadas.
    ("bi",                    [r"ferramenta_de_bi_dia_a_dia", r"ferramenta.*bi_utilizada",
                               r"ferramentas_de_bi.*utiliza", r"bi_utilizada"]),
    ("cloud_preferida",       [r"cloud.*preferida", r"cloud_preferida$"]),
    ("banco",                 [r"bancos_de_dados.*utiliza", r"banco_dados", r"(^|_)banco$"]),

    # ---- bloco 4: inteligencia artificial --------------------------
    # a pergunta certa e "AI generativa e uma prioridade em sua empresa".
    # Existem varias colunas terminando em "n_o_prioridade", que sao itens
    # de checkbox de outra pergunta. Por isso o padrao exige "uma_prioridade"
    ("ia_prioridade",         [r"generativa.*uma_prioridade",
                               r"prioridade_em_sua_empresa", r"ia_prioridade$"]),
    ("uso_ia",                [r"(usa|utiliza)_chatgpt", r"utiliza.*(chatgpt|llm)",
                               r"uso_ia$"]),
    ("motivos_nao_ia",        [r"motivos.*n_o_usar", r"motivos.*(n[a_]o).*(ia|llm|chatgpt)",
                               r"motivos_nao_ia$"]),

    # ---- bloco 5: modelo de trabalho -------------------------------
    ("forma_trabalho_ideal",  [r"(forma|modelo)_de_trabalho_ideal",
                               r"(forma|modelo)_trabalho_ideal$"]),
    ("forma_trabalho",        [r"(forma|modelo)_de_trabalho_atual$",
                               r"forma_de_trabalho$", r"(forma|modelo)_trabalho$"]),
]

# ordem final das colunas na base unificada
COLUNAS_FINAIS = ["ano_pesquisa"] + [nome for nome, _ in PADROES] + [
    "faixa_salarial_ordem", "num_funcionarios_ordem",
    "faixa_salarial_label", "num_funcionarios_label",
    "uso_ia_texto", "usa_ia",
    "ia_prioridade_texto", "ia_prioridade_ordem",
    "motivo_expertise", "motivo_casos_uso", "motivo_dados_prontos",
    "motivo_roi", "motivo_seguranca",
    "usa_sql", "usa_python", "usa_r", "usa_scala",
    "cloud_aws", "cloud_gcp", "cloud_azure",
    "bi_powerbi", "bi_tableau", "bi_looker", "bi_metabase",
    "banco_postgres", "banco_mysql", "banco_sqlserver",
    "banco_bigquery", "banco_databricks", "banco_snowflake", "banco_mongodb",
]


def resolver_colunas(colunas_origem, edicao):
    """Descobre qual coluna original corresponde a cada nome canonico.

    Devolve um dicionario {nome_original: nome_canonico} e imprime
    no log o que casou com o que, para conferencia."""
    disponiveis = list(colunas_origem)
    usados = set()
    mapa = {}
    nao_encontrados = []

    for canonico, padroes in PADROES:
        escolhida = None
        for padrao in padroes:
            candidatas = [c for c in disponiveis
                          if c not in usados and re.search(padrao, c)]
            if candidatas:
                # a de nome mais curto costuma ser a pergunta principal
                candidatas.sort(key=len)
                escolhida = candidatas[0]
                if len(candidatas) > 1:
                    print(f"    [{edicao}] {canonico}: {len(candidatas)} candidatas, "
                          f"escolhi {escolhida} | outras: {candidatas[1:4]}")
                break
        if escolhida:
            mapa[escolhida] = canonico
            usados.add(escolhida)
        else:
            nao_encontrados.append(canonico)

    print(f"\n[{edicao}] resolvidas {len(mapa)} de {len(PADROES)} colunas")
    for original, canonico in sorted(mapa.items(), key=lambda x: x[1]):
        print(f"    {canonico:<24} <- {original}")
    if nao_encontrados:
        print(f"[{edicao}] NAO ENCONTRADAS: {nao_encontrados}")
    return mapa


# =====================================================================
# 2. PADRONIZACAO DE VALORES
#
# Os mapas explicitos abaixo vem da planilha "Mapa de Categorias".
# O que nao estiver neles passa por uma limpeza generica (slug):
# minusculo, sem acento, espaco vira underline.
# =====================================================================
GENERO = {
    "Masculino": "masculino",
    "Feminino": "feminino",
    "Outro": "outro",
    "Prefiro nao informar": "nao_informado",
}

REGIAO = {
    "Sudeste": "sudeste",
    "Sul": "sul",
    "Nordeste": "nordeste",
    "Centro-oeste": "centro_oeste",
    "Norte": "norte",
}

SENIORIDADE = {
    "Junior": "junior",
    "Pleno": "pleno",
    "Senior": "senior",
    "Especialista/Staff+": "especialista",
}

FAIXA_SALARIAL = {
    "Menos de R$ 1.000/mes": "ate_1000",
    "de R$ 1.001/mes a R$ 2.000/mes": "1001_2000",
    "de R$ 101/mes a R$ 2.000/mes": "1001_2000",
    "de R$ 2.001/mes a R$ 3.000/mes": "2001_3000",
    "de R$ 3.001/mes a R$ 4.000/mes": "3001_4000",
    "de R$ 4.001/mes a R$ 6.000/mes": "4001_6000",
    "de R$ 6.001/mes a R$ 8.000/mes": "6001_8000",
    "de R$ 8.001/mes a R$ 12.000/mes": "8001_12000",
    "de R$ 12.001/mes a R$ 16.000/mes": "12001_16000",
    "de R$ 16.001/mes a R$ 20.000/mes": "16001_20000",
    "de R$ 20.001/mes a R$ 25.000/mes": "20001_25000",
    "de R$ 25.001/mes a R$ 30.000/mes": "25001_30000",
    "de R$ 25.001/mes a R$ 3000/mes": "25001_30000",
    "de R$ 30.001/mes a R$ 40.000/mes": "30001_40000",
    "Acima de R$ 40.001/mes": "acima_40001",
}

ORDEM_FAIXA = {
    "ate_1000": 1, "1001_2000": 2, "2001_3000": 3, "3001_4000": 4,
    "4001_6000": 5, "6001_8000": 6, "8001_12000": 7, "12001_16000": 8,
    "16001_20000": 9, "20001_25000": 10, "25001_30000": 11,
    "30001_40000": 12, "acima_40001": 13, "nao_informado": 99,
}

CARGO = {
    "Analista de Dados/Data Analyst": "analista_de_dados",
    "Cientista de Dados/Data Scientist": "cientista_de_dados",
    "Engenheiro de Dados/Arquiteto de Dados/Data Engineer/Data Architect":
        "engenheiro_de_dados",
    "Engenheiro de Dados/Data Engineer/Data Architect": "engenheiro_de_dados",
    "Arquiteto de Dados/Data Architect": "arquiteto_de_dados",
    "Analista de BI/BI Analyst": "analista_de_bi",
    "Analytics Engineer": "analytics_engineer",
    "Analista de Negocios/Business Analyst": "analista_de_negocios",
    "Engenheiro de Machine Learning/ML Engineer/AI Engineer": "engenheiro_de_ml",
    "Desenvolvedor/ Engenheiro de Software/ Analista de Sistemas": "desenvolvedor",
    "Data Product Manager/ Product Manager (PM/APM/DPM/GPM/PO)": "product_manager",
    "Analista de Suporte/Analista Tecnico": "analista_de_suporte",
    "Analista de Inteligencia de Mercado/Market Intelligence":
        "analista_de_inteligencia_mercado",
    "DBA/Administrador de Banco de Dados": "dba",
    "Estatistico": "estatistico",
    "Economista": "economista",
    "Professor/Pesquisador": "professor_pesquisador",
    "Outras Engenharias (nao inclui dev)": "outras_engenharias",
    "Outra Opcao": "outros",
}

MAPA_VALORES = {
    "genero": GENERO,
    "regiao": REGIAO,
    "senioridade": SENIORIDADE,
    "faixa_salarial": FAIXA_SALARIAL,
    "cargo": CARGO,
}

# colunas de escolha unica: passam pelo slug generico
COLUNAS_SLUG = [
    "faixa_idade", "genero", "cor_raca", "pcd", "regiao", "nivel_ensino",
    "area_formacao", "situacao_trabalho", "num_funcionarios", "setor",
    "cargo_gestor", "cargo", "senioridade", "faixa_salarial",
    "exp_dados", "exp_ti", "num_pessoas_dados",
    "linguagem_preferida", "bi_preferida", "cloud_preferida",
    "ia_prioridade", "uso_ia",
]

# colunas de multipla escolha: ficam com o texto original,
# porque uma linha pode conter "SQL, Python, JavaScript"
COLUNAS_TEXTO_LIVRE = [
    "linguagens", "banco", "bi", "funcao_atuacao",
    "motivos_nao_ia", "cargos_time",
]

# forma de trabalho tem regra propria (ver abaixo)
COLUNAS_FORMA = ["forma_trabalho", "forma_trabalho_ideal"]

ACENTOS_DE = "áàâãäéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ"
ACENTOS_PARA = "aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN"

# flags booleanas geradas a partir das colunas de multipla escolha.
# formato: coluna_nova -> (coluna_origem, regex de busca)
FLAGS = {
    "usa_sql":          ("linguagens", r"(?i)(^|,\s*)sql\s*(,|$)"),
    "usa_python":       ("linguagens", r"(?i)python"),
    "usa_r":            ("linguagens", r"(?i)(^|,\s*)r\s*(,|$)"),
    "usa_scala":        ("linguagens", r"(?i)scala"),
    "cloud_aws":        ("cloud_preferida", r"(?i)amazon|aws"),
    "cloud_gcp":        ("cloud_preferida", r"(?i)google\s*cloud|gcp"),
    "cloud_azure":      ("cloud_preferida", r"(?i)azure"),
    "bi_powerbi":       ("bi", r"(?i)power\s*bi"),
    "bi_tableau":       ("bi", r"(?i)tableau"),
    "bi_looker":        ("bi", r"(?i)looker"),
    "bi_metabase":      ("bi", r"(?i)metabase"),
    "banco_postgres":   ("banco", r"(?i)postgre"),
    "banco_mysql":      ("banco", r"(?i)mysql"),
    "banco_sqlserver":  ("banco", r"(?i)sql\s*server"),
    "banco_bigquery":   ("banco", r"(?i)bigquery"),
    "banco_databricks": ("banco", r"(?i)databricks"),
    "banco_snowflake":  ("banco", r"(?i)snowflake"),
    "banco_mongodb":    ("banco", r"(?i)mongo"),
}


# A partir de 2025 a pesquisa parou de guardar as respostas de multipla
# escolha numa coluna de texto e passou a criar uma coluna por opcao
# (4_c_1_sql, 4_c_2_r, 4_c_3_python). Quando a coluna de texto nao existe
# naquela edicao, a flag e preenchida a partir dessas colunas.
FLAGS_DUMMY = {
    "usa_sql":          r"_[0-9]+_sql$",
    "usa_python":       r"_[0-9]+_python$",
    "usa_r":            r"_[0-9]+_r$",
    "usa_scala":        r"_[0-9]+_scala$",
    "bi_powerbi":       r"_[0-9]+_(microsoft_)?power_?bi$",
    "bi_tableau":       r"_[0-9]+_tableau$",
    "bi_looker":        r"_[0-9]+_looker$",
    "bi_metabase":      r"_[0-9]+_metabase$",
    "cloud_aws":        r"_[0-9]+_(amazon|aws)",
    "cloud_gcp":        r"_[0-9]+_google_cloud",
    "cloud_azure":      r"_[0-9]+_azure",
    "banco_postgres":   r"_[0-9]+_postgre",
    "banco_mysql":      r"_[0-9]+_mysql$",
    "banco_sqlserver":  r"_[0-9]+_sql_server$",
    "banco_bigquery":   r"_[0-9]+_(google_)?bigquery$",
    "banco_databricks": r"_[0-9]+_databricks$",
    "banco_snowflake":  r"_[0-9]+_snowflake$",
    "banco_mongodb":    r"_[0-9]+_mongo",
}


def resolver_dummies(colunas_origem, usados, edicao):
    """Acha as colunas de opcao unica (uma por linguagem, uma por
    ferramenta) que substituiram a coluna de texto a partir de 2025."""
    achados = {}
    for flag, padrao in FLAGS_DUMMY.items():
        candidatas = [c for c in colunas_origem
                      if c not in usados and re.search(padrao, c)]
        if candidatas:
            candidatas.sort(key=len)
            achados[flag] = candidatas[0]
    if achados:
        print(f"[{edicao}] colunas de opcao encontradas para {len(achados)} flags")
        for flag, col in sorted(achados.items()):
            print(f"    {flag:<18} <- {col}")
    return achados


def dummy_para_flag(coluna):
    """Converte uma coluna de opcao em sim ou nao, aceitando 1/0,
    Sim/Nao, ou o proprio rotulo da opcao."""
    c = F.lower(F.trim(F.col(coluna).cast(StringType())))
    return (F.when(F.col(coluna).isNull(), None)
             .when(c.isin("1", "true", "sim", "s", "yes"), "sim")
             .when(c.isin("0", "false", "nao", "n", "no", ""), "nao")
             .otherwise("sim"))


def slug(coluna):
    """minusculo, sem acento, sem pontuacao, espaco vira underline"""
    c = F.translate(F.col(coluna), ACENTOS_DE, ACENTOS_PARA)
    c = F.lower(F.trim(c))
    c = F.regexp_replace(c, r"[^a-z0-9]+", "_")
    c = F.regexp_replace(c, r"^_+|_+$", "")
    return c


# =====================================================================
# 3. PROCESSAMENTO DE UMA EDICAO
# =====================================================================
def preparar(edicao):
    caminho = f"{ORIGEM}/ano={edicao}/"
    # pathGlobFilter le apenas os arquivos gerados pelo Visual ETL.
    # Existe um versao_cloudShell.parquet em cada pasta de particao, que
    # e uma copia manual do mesmo dado. Sem esse filtro o Spark le a pasta
    # inteira e cada linha entra duas vezes, dobrando a contagem.
    df = (spark.read
          .option("pathGlobFilter", "run-*.parquet")
          .parquet(caminho))
    print(f"\n{'=' * 60}")
    # nao usar df.count() aqui: forca uma leitura completa da edicao
    # so para imprimir um numero, e triplica o tempo do job
    print(f"[{edicao}] {len(df.columns)} colunas na origem")

    mapa = resolver_colunas(df.columns, edicao)
    dummies = resolver_dummies(df.columns, set(mapa.keys()), edicao)
    for original, canonico in mapa.items():
        df = df.withColumnRenamed(original, canonico)

    df = df.withColumn("ano_pesquisa", F.lit(edicao))

    # cria como nulo o que nao existe nesta edicao
    derivadas = set(FLAGS.keys()) | {"faixa_salarial_ordem"}
    for c in COLUNAS_FINAIS:
        if c not in df.columns and c not in derivadas:
            df = df.withColumn(c, F.lit(None).cast(StringType()))

    todas_texto = COLUNAS_SLUG + COLUNAS_TEXTO_LIVRE + COLUNAS_FORMA + ["uf"]

    # limpa espaco e transforma vazio em nulo
    for c in todas_texto:
        df = df.withColumn(c, F.trim(F.col(c).cast(StringType())))
        df = df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))

    # flags de multipla escolha: calculadas ANTES do slug,
    # porque dependem do texto original com virgulas e espacos
    for nova, (origem, padrao) in FLAGS.items():
        df = df.withColumn(
            nova,
            F.when(F.col(origem).isNull(), None)
             .when(F.col(origem).rlike(padrao), F.lit("sim"))
             .otherwise(F.lit("nao"))
        )

    # onde a coluna de texto nao existe nesta edicao, usa a coluna de
    # opcao. O coalesce mantem o valor do texto quando ele existe.
    for nova, coluna_dummy in dummies.items():
        df = df.withColumn(nova,
                           F.coalesce(F.col(nova), dummy_para_flag(coluna_dummy)))

    for c in COLUNAS_SLUG:
        df = df.withColumn(c, slug(c))

    import unicodedata as U

    def _sl(t):
        t = U.normalize("NFKD", t).encode("ascii", "ignore").decode()
        return re.sub(r"[^a-z0-9]+", "_", t.lower().strip()).strip("_")

    for coluna, mv in MAPA_VALORES.items():
        df = df.replace({_sl(k): v for k, v in mv.items()}, subset=[coluna])

    nf = F.col("num_funcionarios")
    df = df.withColumn("num_funcionarios_ordem",
        F.when(nf == "de_1_a_5", 1)
         .when(nf == "de_6_a_10", 2)
         .when(nf == "de_11_a_50", 3)
         .when(nf == "de_51_a_100", 4)
         .when(nf == "de_101_a_500", 5)
         .when(nf == "de_501_a_1_000", 6)
         .when(nf == "de_501_a_100", 6)
         .when(nf == "de_1_001_a_3_000", 7)
         .when(nf == "acima_de_3_000", 8)
         .otherwise(99))

    fs = F.col("faixa_salarial")
    df = df.withColumn("faixa_salarial_label",
        F.when(fs == "ate_1000", "R$ 0 a R$ 1.000")
         .when(fs == "1001_2000", "R$ 1.001 a R$ 2.000")
         .when(fs == "2001_3000", "R$ 2.001 a R$ 3.000")
         .when(fs == "3001_4000", "R$ 3.001 a R$ 4.000")
         .when(fs == "4001_6000", "R$ 4.001 a R$ 6.000")
         .when(fs == "6001_8000", "R$ 6.001 a R$ 8.000")
         .when(fs == "8001_12000", "R$ 8.001 a R$ 12.000")
         .when(fs == "12001_16000", "R$ 12.001 a R$ 16.000")
         .when(fs == "16001_20000", "R$ 16.001 a R$ 20.000")
         .when(fs == "20001_25000", "R$ 20.001 a R$ 25.000")
         .when(fs == "25001_30000", "R$ 25.001 a R$ 30.000")
         .when(fs == "30001_40000", "R$ 30.001 a R$ 40.000")
         .when(fs == "acima_40001", "R$ 40.001 ou mais")
         .otherwise("Sem resposta"))

    df = df.withColumn("num_funcionarios_label",
        F.when(nf == "de_1_a_5", "1 a 5")
         .when(nf == "de_6_a_10", "6 a 10")
         .when(nf == "de_11_a_50", "11 a 50")
         .when(nf == "de_51_a_100", "51 a 100")
         .when(nf == "de_101_a_500", "101 a 500")
         .when(nf == "de_501_a_1_000", "501 a 1.000")
         .when(nf == "de_501_a_100", "501 a 1.000")
         .when(nf == "de_1_001_a_3_000", "1.001 a 3.000")
         .when(nf == "acima_de_3_000", "3.000 ou mais")
         .otherwise("Sem resposta"))

    ui = F.col("uso_ia")
    df = df.withColumn("uso_ia_texto", ui)
    df = df.withColumn("usa_ia",
        F.when(ui.isNull() | (ui == "nao_informado"), "nao_informado")
         .when(ui.contains("nao_utilizo_nenhum"), "nao")
         .otherwise("sim"))
    df = df.withColumn("uso_ia",
        F.when(ui.contains("paga_pela_solucao"), "pago_empresa")
         .when(ui.contains("proprio_bolso"), "pago_proprio")
         .when(ui.contains("copilot"), "codigo")
         .when(ui.contains("para_codigo"), "codigo")
         .when(ui.contains("solucoes_gratuitas"), "gratuito")
         .when(ui.contains("nao_utilizo_nenhum"), "nao_usa")
         .otherwise("nao_informado"))

    ip = F.col("ia_prioridade")
    df = df.withColumn("ia_prioridade_texto", ip)
    df = df.withColumn("ia_prioridade_ordem",
        F.when(ip.contains("nossa_principal_prioridade"), 1)
         .when(ip.contains("entre_nossas_principais_prioridades"), 2)
         .when(ip.contains("mais_ou_menos"), 3)
         .when(ip.contains("nao_e_uma_iniciativa"), 4)
         .when(ip.contains("nao_sei_opinar"), 5)
         .otherwise(99))
    df = df.withColumn("ia_prioridade",
        F.when(ip.contains("nossa_principal_prioridade"), "foco_principal")
         .when(ip.contains("entre_nossas_principais_prioridades"), "prioritaria")
         .when(ip.contains("mais_ou_menos"), "parcial")
         .when(ip.contains("nao_e_uma_iniciativa"), "nao_prioridade")
         .when(ip.contains("nao_sei_opinar"), "nao_sei")
         .otherwise("nao_informado"))

    mt = F.col("motivos_nao_ia")
    for nova_col, termo in [("motivo_expertise", "expertise"),
                            ("motivo_casos_uso", "compreens"),
                            ("motivo_dados_prontos", "prontos para uso"),
                            ("motivo_roi", "retorno sobre investimento"),
                            ("motivo_seguranca", "seguran")]:
        df = df.withColumn(nova_col,
            F.when(mt.isNull(), "nao_informado")
             .when(F.lower(mt).contains(termo), "sim")
             .otherwise("nao"))

    # forma de trabalho: regra de "contem", porque uma das opcoes
    # e uma frase longa que quebra ao colar no editor do Glue
    for c in COLUNAS_FORMA:
        col = F.col(c)
        df = df.withColumn(
            c,
            F.when(col.contains("100% remoto"), "remoto")
             .when(col.contains("100% presencial"), "presencial")
             .when(col.contains("dias fixos"), "hibrido_dias_fixos")
             .when(col.contains("flex"), "hibrido_flexivel")
             .otherwise(col)
        )

    df = df.withColumn("uf", F.lower(F.trim(F.col("uf"))))
    df = df.withColumn("idade", F.col("idade").cast("int"))

    # nulo vira nao_informado, para o grafico nao perder a categoria
    for c in COLUNAS_SLUG + COLUNAS_FORMA + ["uf"] + list(FLAGS.keys()):
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit("nao_informado")))

    # ordem numerica da faixa salarial
    expr = F.create_map([F.lit(x) for kv in ORDEM_FAIXA.items() for x in kv])
    df = df.withColumn("faixa_salarial_ordem",
                       F.coalesce(expr[F.col("faixa_salarial")], F.lit(99)))

    return df.select(*COLUNAS_FINAIS)


dfs = [preparar(e) for e in EDICOES]

unificado = dfs[0]
for d in dfs[1:]:
    unificado = unificado.unionByName(d, allowMissingColumns=True)

print(f"\nColunas na base unificada: {len(unificado.columns)}")

# =====================================================================
# 4. GRAVA A BASE UNIFICADA
#
# A gravacao vem ANTES do relatorio de proposito. Se o relatorio
# falhar, o resultado principal ja esta salvo no S3 e nao se perde
# o processamento inteiro.
# =====================================================================
(unificado
   .repartition("ano_pesquisa")
   .write
   .mode("overwrite")
   .partitionBy("ano_pesquisa")
   .parquet(DESTINO))

print("Base unificada gravada em:", DESTINO)

# contagem por ano lida do resultado ja gravado, e barato
contagem = (spark.read.parquet(DESTINO)
            .groupBy("ano_pesquisa").count().orderBy("ano_pesquisa").collect())
for linha in contagem:
    print(f"    {linha['ano_pesquisa']}: {linha['count']} linhas")

# =====================================================================
# 5. RELATORIO DE COBERTURA
#
# Cinco colunas da pesquisa (num_pessoas_dados, cargos_time,
# ia_prioridade, uso_ia, motivos_nao_ia) so foram respondidas por
# gestores, entao passam de 80 por cento de nao_informado.
# Isso nao e erro do pipeline, e caracteristica da pesquisa, mas
# precisa aparecer na apresentacao junto com qualquer percentual
# calculado sobre essas colunas.
#
# IMPORTANTE: o relatorio e montado em UMA passada com stack(), nao
# com um union por coluna. Um union por coluna gera um plano de
# execucao enorme e derruba o driver com OUT_OF_MEMORY_ERROR.
# =====================================================================
print("\nGerando relatorio de cobertura por ano...")

# le de volta do S3 em vez de reaproveitar o dataframe em memoria
base = spark.read.parquet(DESTINO)

colunas_relatorio = [c for c in base.columns
                     if c not in ("ano_pesquisa", "id", "idade",
                                  "faixa_salarial_ordem")]

agregacoes = [F.count("*").alias("total")]
for c in colunas_relatorio:
    agregacoes.append(
        F.sum(F.when(F.col(c).isNull() | (F.col(c) == "nao_informado"), 1)
               .otherwise(0)).alias(f"sem__{c}")
    )

resumo = base.groupBy("ano_pesquisa").agg(*agregacoes)

pares = ", ".join([f"'{c}', `sem__{c}`" for c in colunas_relatorio])
stack_expr = f"stack({len(colunas_relatorio)}, {pares}) as (coluna, sem_resposta)"

cobertura = (resumo
             .select("ano_pesquisa", "total", F.expr(stack_expr))
             .withColumn("pct_sem_resposta",
                         F.round(100.0 * F.col("sem_resposta") / F.col("total"), 1))
             .select("coluna", "ano_pesquisa", "total",
                     "sem_resposta", "pct_sem_resposta"))

(cobertura
   .coalesce(1)
   .write
   .mode("overwrite")
   .option("header", True)
   .csv(DESTINO_COBERTURA))

print("Relatorio de cobertura gravado em:", DESTINO_COBERTURA)
print("\nConcluido.")
