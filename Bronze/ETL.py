import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

# Script generated for node limpa_2025_2026
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    import re
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import lit

    df = dfc.select(list(dfc.keys())[0]).toDF()

    def limpar(nome):
        n = nome.strip().lower()
        n = re.sub(r"[^0-9a-z]+", "_", n)
        n = re.sub(r"_+", "_", n).strip("_")
        return n or "coluna_sem_nome"

    vistos, novos = {}, []
    for c in df.columns:
        n = limpar(c)
        if n in vistos:
            vistos[n] += 1
            n = f"{n}_{vistos[n]}"
        else:
            vistos[n] = 0
        novos.append(n)

    df = df.toDF(*novos).withColumn("ano_pesquisa", lit("2025_2026"))

    dyf = DynamicFrame.fromDF(df, glueContext, "limpo")
    return DynamicFrameCollection({"saida": dyf}, glueContext)
# Script generated for node limpa_2024_2025
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    import re
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import lit

    df = dfc.select(list(dfc.keys())[0]).toDF()

    def limpar(nome):
        n = nome.strip().lower()
        n = re.sub(r"[^0-9a-z]+", "_", n)
        n = re.sub(r"_+", "_", n).strip("_")
        return n or "coluna_sem_nome"

    vistos, novos = {}, []
    for c in df.columns:
        n = limpar(c)
        if n in vistos:
            vistos[n] += 1
            n = f"{n}_{vistos[n]}"
        else:
            vistos[n] = 0
        novos.append(n)

    df = df.toDF(*novos).withColumn("ano_pesquisa", lit("2024_2025"))

    dyf = DynamicFrame.fromDF(df, glueContext, "limpo")
    return DynamicFrameCollection({"saida": dyf}, glueContext)
# Script generated for node limpa_2023_2024
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    import re
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import lit

    df = dfc.select(list(dfc.keys())[0]).toDF()

    def limpar(nome):
        n = nome.strip().lower()
        n = re.sub(r"[^0-9a-z]+", "_", n)
        n = re.sub(r"_+", "_", n).strip("_")
        return n or "coluna_sem_nome"

    vistos, novos = {}, []
    for c in df.columns:
        n = limpar(c)
        if n in vistos:
            vistos[n] += 1
            n = f"{n}_{vistos[n]}"
        else:
            vistos[n] = 0
        novos.append(n)

    df = df.toDF(*novos).withColumn("ano_pesquisa", lit("2023_2024"))

    dyf = DynamicFrame.fromDF(df, glueContext, "limpo")
    return DynamicFrameCollection({"saida": dyf}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node edicao_2024_2025
edicao_2024_2025_node1786744077967 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://tc-fase3-state-of-data-seu-id/bronze/state_of_data/ano=2024_2025/State of Data Brazil 2024-2025.csv"], "recurse": True}, transformation_ctx="edicao_2024_2025_node1786744077967")

# Script generated for node edicao_2025_2026
edicao_2025_2026_node1786744079284 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://tc-fase3-state-of-data-seu-id/bronze/state_of_data/ano=2025_2026/State of Data Brazil 2025-2026.csv"], "recurse": True}, transformation_ctx="edicao_2025_2026_node1786744079284")

# Script generated for node edicao_2023_2024
edicao_2023_2024_node1786744058735 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://tc-fase3-state-of-data-seu-id/bronze/state_of_data/ano=2023_2024/State of Data Brazil 2023-2024.csv"]}, transformation_ctx="edicao_2023_2024_node1786744058735")

# Script generated for node limpa_2024_2025
limpa_2024_2025_node1786745945258 = MyTransform(glueContext, DynamicFrameCollection({"edicao_2024_2025_node1786744077967": edicao_2024_2025_node1786744077967}, glueContext))

# Script generated for node limpa_2025_2026
limpa_2025_2026_node1786746416872 = MyTransform(glueContext, DynamicFrameCollection({"edicao_2025_2026_node1786744079284": edicao_2025_2026_node1786744079284}, glueContext))

# Script generated for node limpa_2023_2024
limpa_2023_2024_node1786744786048 = MyTransform(glueContext, DynamicFrameCollection({"edicao_2023_2024_node1786744058735": edicao_2023_2024_node1786744058735}, glueContext))

# Script generated for node seleciona_2024_2025
seleciona_2024_2025_node1786746061441 = SelectFromCollection.apply(dfc=limpa_2024_2025_node1786745945258, key=list(limpa_2024_2025_node1786745945258.keys())[0], transformation_ctx="seleciona_2024_2025_node1786746061441")

# Script generated for node seleciona_202_2026
seleciona_202_2026_node1786746451321 = SelectFromCollection.apply(dfc=limpa_2025_2026_node1786746416872, key=list(limpa_2025_2026_node1786746416872.keys())[0], transformation_ctx="seleciona_202_2026_node1786746451321")

# Script generated for node seleciona_2023_2024
seleciona_2023_2024_node1786744966230 = SelectFromCollection.apply(dfc=limpa_2023_2024_node1786744786048, key=list(limpa_2023_2024_node1786744786048.keys())[0], transformation_ctx="seleciona_2023_2024_node1786744966230")

# Script generated for node parquet_2024_2025
EvaluateDataQuality().process_rows(frame=seleciona_2024_2025_node1786746061441, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1786745863326", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
parquet_2024_2025_node1786746081191 = glueContext.getSink(path="s3://tc-fase3-state-of-data-seu-id/silver/state_of_data/ano=2024_2025/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="parquet_2024_2025_node1786746081191")
parquet_2024_2025_node1786746081191.setCatalogInfo(catalogDatabase="db_tc_fase3",catalogTableName="table_2024_2025")
parquet_2024_2025_node1786746081191.setFormat("glueparquet", compression="snappy")
parquet_2024_2025_node1786746081191.writeFrame(seleciona_2024_2025_node1786746061441)
# Script generated for node parquet_2025_2026
EvaluateDataQuality().process_rows(frame=seleciona_202_2026_node1786746451321, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1786745863326", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
parquet_2025_2026_node1786746470388 = glueContext.getSink(path="s3://tc-fase3-state-of-data-seu-id/silver/state_of_data/ano=2025_2026/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="parquet_2025_2026_node1786746470388")
parquet_2025_2026_node1786746470388.setCatalogInfo(catalogDatabase="db_tc_fase3",catalogTableName="table_2025_2026")
parquet_2025_2026_node1786746470388.setFormat("glueparquet", compression="snappy")
parquet_2025_2026_node1786746470388.writeFrame(seleciona_202_2026_node1786746451321)
# Script generated for node parquet_2023_2024
EvaluateDataQuality().process_rows(frame=seleciona_2023_2024_node1786744966230, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1786743937736", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
parquet_2023_2024_node1786745009679 = glueContext.getSink(path="s3://tc-fase3-state-of-data-seu-id/silver/state_of_data/ano=2023_2024/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="parquet_2023_2024_node1786745009679")
parquet_2023_2024_node1786745009679.setCatalogInfo(catalogDatabase="db_tc_fase3",catalogTableName="table_2023_2024")
parquet_2023_2024_node1786745009679.setFormat("glueparquet", compression="snappy")
parquet_2023_2024_node1786745009679.writeFrame(seleciona_2023_2024_node1786744966230)
job.commit()