# =====================================================================
# Tech Challenge Fase 3 - Etapa 4
# Job: Silver por edicao  ->  Silver unificada
#
# O que faz:
#   1. le as 3 edicoes gravadas em Parquet na camada Silver
#   2. renomeia as colunas para o nome canonico definido na Etapa 3
#   3. padroniza os valores das categorias
#   4. une as 3 numa base unica e grava em silver_final
#
# Fonte dos mapeamentos: planilha "Mapa de Categorias - State of Data.xlsx"
# =====================================================================

import sys
from awsglue.utils import getResolvedOptions
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

# ---------------------------------------------------------------------
# 1. MAPA DE COLUNAS  (aba mapa_colunas da planilha)
#    chave = nome original naquele ano, valor = nome canonico
# ---------------------------------------------------------------------
MAPA_COLUNAS = {
    "2023_2024": {
        "p1_b_genero": "genero",
        "p1_i_1_uf_onde_mora": "uf",
        "p1_i_2_regiao_onde_mora": "regiao",
        "p2_f_cargo_atual": "cargo",
        "p2_g_nivel": "senioridade",
        "p2_h_faixa_salarial": "faixa_salarial",
        "p2_r_atualmente_qual_a_sua_forma_de_trabalho": "forma_trabalho",
    },
    "2024_2025": {
        "1_b_genero": "genero",
        "1_i_1_uf_onde_mora": "uf",
        "1_i_2_regiao_onde_mora": "regiao",
        "2_f_cargo_atual": "cargo",
        "2_g_nivel": "senioridade",
        "2_h_faixa_salarial": "faixa_salarial",
        "2_r_modelo_de_trabalho_atual": "forma_trabalho",
    },
    "2025_2026": {
        "1_b_genero": "genero",
        "1_i_1_uf_onde_mora": "uf",
        "1_i_2_regiao_onde_mora": "regiao",
        "2_f_cargo_atual": "cargo",
        "2_g_nivel": "senioridade",
        "2_h_faixa_salarial": "faixa_salarial",
        # ATENCAO: em 2025_2026 a letra mudou de r para q
        "2_q_modelo_de_trabalho_atual": "forma_trabalho",
    },
}

# ordem final das colunas na base unificada
COLUNAS_FINAIS = [
    "ano_pesquisa",
    "genero",
    "uf",
    "regiao",
    "cargo",
    "senioridade",
    "faixa_salarial",
    "faixa_salarial_ordem",
    "forma_trabalho",
]

# ---------------------------------------------------------------------
# 2. MAPA DE CATEGORIAS  (aba mapa_categorias da planilha)
# ---------------------------------------------------------------------
GENERO = {
    "Masculino": "masculino",
    "Feminino": "feminino",
    "Outro": "outro",
    "Prefiro não informar": "nao_informado",
}

REGIAO = {
    "Sudeste": "sudeste",
    "Sul": "sul",
    "Nordeste": "nordeste",
    "Centro-oeste": "centro_oeste",
    "Norte": "norte",
}

SENIORIDADE = {
    "Júnior": "junior",
    "Pleno": "pleno",
    "Sênior": "senior",
    "Especialista/Staff+": "especialista",
}

# forma de trabalho e tratada por regra de "contem" mais abaixo,
# porque uma das opcoes e uma frase muito longa e quebra ao colar no editor

FAIXA_SALARIAL = {
    "Menos de R$ 1.000/mês": "ate_1000",
    "de R$ 1.001/mês a R$ 2.000/mês": "1001_2000",
    "de R$ 101/mês a R$ 2.000/mês": "1001_2000",          # erro de digitacao da pesquisa
    "de R$ 2.001/mês a R$ 3.000/mês": "2001_3000",
    "de R$ 3.001/mês a R$ 4.000/mês": "3001_4000",
    "de R$ 4.001/mês a R$ 6.000/mês": "4001_6000",
    "de R$ 6.001/mês a R$ 8.000/mês": "6001_8000",
    "de R$ 8.001/mês a R$ 12.000/mês": "8001_12000",
    "de R$ 12.001/mês a R$ 16.000/mês": "12001_16000",
    "de R$ 16.001/mês a R$ 20.000/mês": "16001_20000",
    "de R$ 20.001/mês a R$ 25.000/mês": "20001_25000",
    "de R$ 25.001/mês a R$ 30.000/mês": "25001_30000",
    "de R$ 25.001/mês a R$ 3000/mês": "25001_30000",      # erro de digitacao da pesquisa
    "de R$ 30.001/mês a R$ 40.000/mês": "30001_40000",
    "Acima de R$ 40.001/mês": "acima_40001",
}

# ordem numerica da faixa salarial, para os graficos sairem na ordem certa
ORDEM_FAIXA = {
    "ate_1000": 1, "1001_2000": 2, "2001_3000": 3, "3001_4000": 4,
    "4001_6000": 5, "6001_8000": 6, "8001_12000": 7, "12001_16000": 8,
    "16001_20000": 9, "20001_25000": 10, "25001_30000": 11,
    "30001_40000": 12, "acima_40001": 13, "nao_informado": 99,
}

CARGO = {
    "Analista de Dados/Data Analyst": "analista_de_dados",
    "Cientista de Dados/Data Scientist": "cientista_de_dados",
    "Engenheiro de Dados/Arquiteto de Dados/Data Engineer/Data Architect": "engenheiro_de_dados",
    "Engenheiro de Dados/Data Engineer/Data Architect": "engenheiro_de_dados",
    "Arquiteto de Dados/Data Architect": "arquiteto_de_dados",
    "Analista de BI/BI Analyst": "analista_de_bi",
    "Analytics Engineer": "analytics_engineer",
    "Analista de Negócios/Business Analyst": "analista_de_negocios",
    "Engenheiro de Machine Learning/ML Engineer/AI Engineer": "engenheiro_de_ml",
    "Desenvolvedor/ Engenheiro de Software/ Analista de Sistemas": "desenvolvedor",
    "Data Product Manager/ Product Manager (PM/APM/DPM/GPM/PO)": "product_manager",
    "Analista de Suporte/Analista Técnico": "analista_de_suporte",
    "Analista de Inteligência de Mercado/Market Intelligence": "analista_de_inteligencia_mercado",
    "DBA/Administrador de Banco de Dados": "dba",
    "Estatístico": "estatistico",
    "Economista": "economista",
    "Professor/Pesquisador": "professor_pesquisador",
    "Outras Engenharias (não inclui dev)": "outras_engenharias",
    "Outra Opção": "outros",
}

MAPA_VALORES = {
    "genero": GENERO,
    "regiao": REGIAO,
    "senioridade": SENIORIDADE,
    "faixa_salarial": FAIXA_SALARIAL,
    "cargo": CARGO,
}

# colunas que passam pela limpeza de espaco e pelo coalesce final
COLUNAS_TEXTO = list(MAPA_VALORES.keys()) + ["forma_trabalho", "uf"]

# ---------------------------------------------------------------------
# 3. PROCESSAMENTO
# ---------------------------------------------------------------------
def preparar(edicao):
    """Le uma edicao, renomeia as colunas e padroniza os valores."""
    caminho = f"{ORIGEM}/ano={edicao}/"
    df = spark.read.parquet(caminho)
    print(f"[{edicao}] lidas {df.count()} linhas com {len(df.columns)} colunas")

    # renomeia para o nome canonico
    for original, canonico in MAPA_COLUNAS[edicao].items():
        if original in df.columns:
            df = df.withColumnRenamed(original, canonico)
        else:
            print(f"[{edicao}] AVISO: coluna {original} nao encontrada")

    # garante que a coluna de ano existe e esta certa
    df = df.withColumn("ano_pesquisa", F.lit(edicao))

    # cria como nulo qualquer coluna canonica que falte nesta edicao
    for c in COLUNAS_FINAIS:
        if c not in df.columns and c != "faixa_salarial_ordem":
            print(f"[{edicao}] criando {c} como nulo")
            df = df.withColumn(c, F.lit(None).cast(StringType()))

    # limpa espacos e troca vazio por nulo
    for c in COLUNAS_TEXTO:
        df = df.withColumn(c, F.trim(F.col(c)))
        df = df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))

    # aplica o mapa de categorias
    for coluna, mapa in MAPA_VALORES.items():
        df = df.replace(mapa, subset=[coluna])

    # forma de trabalho por regra de "contem", evita string longa no codigo
    ft = F.col("forma_trabalho")
    df = df.withColumn(
        "forma_trabalho",
        F.when(ft.contains("100% remoto"), "remoto")
         .when(ft.contains("100% presencial"), "presencial")
         .when(ft.contains("dias fixos"), "hibrido_dias_fixos")
         .when(ft.contains("flex"), "hibrido_flexivel")
         .otherwise(ft)
    )

    # uf em minusculo
    df = df.withColumn("uf", F.lower(F.trim(F.col("uf"))))

    # nulo vira nao_informado, para o grafico nao perder a categoria
    for c in COLUNAS_TEXTO:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit("nao_informado")))

    # coluna de ordem da faixa salarial
    expr = F.create_map([F.lit(x) for kv in ORDEM_FAIXA.items() for x in kv])
    df = df.withColumn("faixa_salarial_ordem",
                       F.coalesce(expr[F.col("faixa_salarial")], F.lit(99)))

    return df.select(*COLUNAS_FINAIS)

edicoes = ["2023_2024", "2024_2025", "2025_2026"]
dfs = [preparar(e) for e in edicoes]

unificado = dfs[0]
for d in dfs[1:]:
    unificado = unificado.unionByName(d, allowMissingColumns=True)

total = unificado.count()
print(f"TOTAL UNIFICADO: {total} linhas")
unificado.groupBy("ano_pesquisa").count().orderBy("ano_pesquisa").show(truncate=False)

# ---------------------------------------------------------------------
# 4. GRAVA
# ---------------------------------------------------------------------
(unificado
   .repartition("ano_pesquisa")
   .write
   .mode("overwrite")
   .partitionBy("ano_pesquisa")
   .parquet(DESTINO))

print("Concluido. Destino:", DESTINO)