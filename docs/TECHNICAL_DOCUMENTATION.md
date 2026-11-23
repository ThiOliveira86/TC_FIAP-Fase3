# Documentação Técnica do Pipeline de Dados

**Tech Challenge - Fase 3**
Pós-Graduação em Data Analytics - FIAP

Base de dados: State of Data Brazil, edições 2023/2024, 2024/2025 e 2025/2026
Repositório de dados: `s3://tc-fase3-state-of-data

---

## Sumário

1. [Objetivo do documento](#1-objetivo-do-documento)
2. [Escopo e artefatos documentados](#2-escopo-e-artefatos-documentados)
3. [Arquitetura da solução](#3-arquitetura-da-solução)
4. [Fundamentação do problema](#4-fundamentação-do-problema)
5. [Artefato 1: ETL.py](#5-artefato-1-etlpy)
6. [Artefato 2: padronizacao_silver.py](#6-artefato-2-padronizacao_silverpy)
7. [Artefato 3: job_silver_unificada_v2.py](#7-artefato-3-job_silver_unificada_v2py)
8. [Artefato 4: TechChallenge_Notebook_Unificado_v4.ipynb](#8-artefato-4-techchallenge_notebook_unificado_v4ipynb)
9. [Resultados e validação](#9-resultados-e-validação)
10. [Limitações conhecidas](#10-limitações-conhecidas)
11. [Procedimento de reexecução](#11-procedimento-de-reexecução)
12. [Diagnóstico de falhas](#12-diagnóstico-de-falhas)
13. [Procedimento para inclusão de novos atributos](#13-procedimento-para-inclusão-de-novos-atributos)

---

## 1. Objetivo do documento

O presente documento descreve os artefatos de código que compõem o pipeline
de dados do Tech Challenge da Fase 3, desde a ingestão do dado bruto até a
geração da camada analítica que responde às sete perguntas de negócio
formuladas no enunciado.

Para cada artefato são detalhados a função no pipeline, as decisões técnicas
adotadas em sua construção e os critérios de validação empregados. A
documentação registra igualmente a evolução da solução, uma vez que a etapa
de unificação foi implementada em duas versões sucessivas. A comparação
entre ambas constitui parte relevante da fundamentação técnica do trabalho,
pois evidencia a limitação identificada na primeira abordagem e a
justificativa para sua substituição.

---

## 2. Escopo e artefatos documentados

São documentados quatro artefatos, correspondentes a três etapas distintas
do pipeline, conforme o Quadro 1.

**Quadro 1 - Artefatos documentados**

| Ordem | Artefato | Ambiente de execução | Entrada | Saída |
|:---:|---|---|---|---|
| 1 | `ETL.py` | AWS Glue Visual ETL | 3 arquivos CSV na camada Bronze | Parquet na camada Silver, segregado por edição |
| 2 | `padronizacao_silver.py` | AWS Glue Job (Script Editor) | Camada Silver por edição | Base unificada com 9 atributos |
| 3 | `job_silver_unificada_v2.py` | AWS Glue Job (Script Editor) | Camada Silver por edição | Base unificada com 64 atributos |
| 4 | `TechChallenge_Notebook_Unificado_v4.ipynb` | Jupyter, PySpark local | Base unificada | Camada Gold: agregações e insumos gráficos |

Os artefatos 2 e 3 executam a mesma etapa lógica do pipeline. O artefato 2
corresponde à implementação inicial e encontra-se documentado por sua
relevância metodológica. **A versão em produção é o artefato 3.**

### 2.1 Organização da entrega

Os artefatos encontram-se distribuídos em diretórios nomeados segundo a
camada da arquitetura Medallion a que se vinculam:

```
.
├── README.md                                     documento presente
├── Bronze/
│   └── ETL.py                                    ingestão e conversão para Parquet
├── Silver/
│   ├── job_silver_unificada_v2.py                unificação, versão em produção
│   └── padronizacao_silver.py                    unificação, versão inicial
└── Gold/
    └── TechChallenge_Notebook_Unificado_v4.ipynb agregações analíticas
```

O diretório `Bronze/` abriga o artefato que consome a camada homônima; os
arquivos CSV que a compõem foram obtidos no Kaggle e carregados manualmente
no Amazon S3, sem transformação, razão pela qual não há script de ingestão.

---

## 3. Arquitetura da solução

A solução adota a arquitetura Medallion, organizada em camadas de
refinamento progressivo, integralmente sobre serviços gerenciados da AWS.

```
Fonte externa (Kaggle)
        |
        v
[Bronze]  s3://.../bronze/state_of_data/ano=*/
          Dado bruto em CSV, preservado sem transformação
        |
        |  ETL.py  (AWS Glue Visual ETL)
        v
[Silver]  s3://.../silver/state_of_data/ano=*/
          Parquet com nomes de coluna normalizados, uma pasta por edição
        |
        |  job_silver_unificada_v2.py  (AWS Glue Job)
        v
[Silver final]  s3://.../silver_final/state_of_data_unificado/
          Base única, esquema canônico, particionada por ano_pesquisa
        |
        +--> Glue Crawler --> Glue Data Catalog --> Amazon Athena
        |
        |  TechChallenge_Notebook_Unificado_v4.ipynb  (Jupyter, PySpark)
        v
[Gold]    s3://.../gold/
          Agregações analíticas e insumos gráficos
```

A separação entre as camadas obedece ao princípio de responsabilidade
única: a camada Bronze preserva a rastreabilidade em relação à fonte, a
camada Silver concentra a padronização estrutural e semântica, e a camada
Gold responde às perguntas de negócio formuladas no enunciado do desafio.

---

## 4. Fundamentação do problema

### 4.1 Constatação empírica

As três edições da pesquisa, embora derivadas do mesmo instrumento de
coleta, não apresentam esquema compatível entre si. Tal incompatibilidade
foi verificada empiricamente mediante a execução de um AWS Glue Crawler
sobre os três conjuntos de dados: caso os esquemas fossem equivalentes, o
serviço registraria uma única tabela particionada por ano. O resultado
observado foi o registro de três tabelas distintas, o que caracteriza
formalmente a ocorrência de deriva de esquema (*schema drift*).

### 4.2 Tipologia das divergências

A análise comparativa dos três esquemas permitiu classificar as
divergências em quatro padrões, apresentados no Quadro 2.

**Quadro 2 - Padrões de divergência entre edições**

| Padrão | Descrição | Exemplo |
|:---:|---|---|
| I | Supressão do prefixo identificador | `p1_b_genero` passa a `1_b_genero` a partir de 2024 |
| II | Reformulação do enunciado da questão | `atualmente_qual_a_sua_forma_de_trabalho` passa a `modelo_de_trabalho_atual` |
| III | Deslocamento do código alfabético | `2_r` passa a `2_q`; `4_j` passa a `4_g`; `4_m` passa a `4_j`; `3_g` passa a `3_h` |
| IV | Alteração da estrutura de resposta | Em 2025, a questão de linguagens deixa de ser atributo textual único e passa a um atributo por linguagem (`4_c_1_sql`, `4_c_2_r`, `4_c_3_python`) |

O padrão III merece destaque metodológico. A inserção ou remoção de uma
questão no instrumento de coleta desloca em uma posição o código alfabético
de todas as questões subsequentes. Trata-se, portanto, de uma divergência
sistemática e não pontual, cuja recorrência foi confirmada em quatro
atributos distintos.

### 4.3 Diretriz técnica decorrente

Da constatação acima decorre a diretriz adotada no projeto e comunicada aos
demais integrantes do grupo: **a identificação de um atributo não deve
apoiar-se em seu código alfabético**. O código não constitui identificador
estável entre edições.

---

## 5. Artefato 1: `ETL.py`

### 5.1 Caracterização

Trata-se do script gerado automaticamente pelo AWS Glue Visual ETL,
responsável pela transição da camada Bronze para a camada Silver. Por ser
produto da ferramenta gráfica, o código apresenta a estrutura característica
de nós numerados e identificadores gerados por *timestamp*.

### 5.2 Estrutura de execução

O job compreende três ramos independentes, um por edição, cada qual composto
por quatro nós:

1. **Nó de origem.** Realiza a leitura do arquivo CSV da camada Bronze,
   parametrizado com `withHeader=True` e `quoteChar='"'`. A definição
   explícita do caractere delimitador de texto é imprescindível, uma vez
   que diversas respostas da pesquisa contêm vírgulas em seu conteúdo; sua
   omissão acarretaria segmentação incorreta dos registros.

2. **Nó de transformação** (`MyTransform`). Normaliza os identificadores das
   colunas e atribui o rótulo da edição.

3. **Nó de seleção** (`SelectFromCollection`). Extrai o `DynamicFrame` único
   contido na coleção retornada pelo nó anterior.

4. **Nó de destino.** Persiste o resultado em formato Parquet com compressão
   Snappy e registra a tabela no Glue Data Catalog mediante o parâmetro
   `enableUpdateCatalog=True`.

### 5.3 Normalização dos identificadores de coluna

A função de normalização é reproduzida na Listagem 1.

**Listagem 1 - Rotina de normalização de identificadores**

```python
def limpar(nome):
    n = nome.strip().lower()
    n = re.sub(r"[^0-9a-z]+", "_", n)
    n = re.sub(r"_+", "_", n).strip("_")
    return n or "coluna_sem_nome"
```

A rotina converte o identificador para caixa baixa, substitui por
sublinhado todo caractere que não seja letra minúscula ou algarismo, e
reduz sequências consecutivas de sublinhados a uma única ocorrência. Um
contador auxiliar (`vistos`) resolve eventuais colisões de nome mediante a
aposição de sufixo numérico.

**Implicação técnica relevante.** A classe de caracteres `[0-9a-z]` não
contempla caracteres acentuados. Consequentemente, todo acento é convertido
em sublinhado: o identificador `Área de Formação` resulta em
`_rea_de_forma_o`. Esta característica condiciona diretamente a construção
das expressões regulares do artefato 3, conforme detalhado na Seção 7.2, e
foi responsável pela falha de resolução de cinco atributos em sua primeira
execução.

### 5.4 Destinos de gravação

Os destinos de gravação por edição constam do Quadro 3.

**Quadro 3 - Destinos por edição**

| Edição | Caminho na camada Silver | Tabela registrada |
|---|---|---|
| 2023_2024 | `silver/state_of_data/ano=2023_2024/` | `table_2023_2024` |
| 2024_2025 | `silver/state_of_data/ano=2024_2025/` | `table_2024_2025` |
| 2025_2026 | `silver/state_of_data/ano=2025_2026/` | `table_2025_2026` |

Banco de dados no Glue Data Catalog: `db_tc_fase3`.

### 5.5 Defeito identificado e sem efeito operacional

O AWS Glue Visual ETL gera uma definição de função por nó de transformação,
todas homônimas (`MyTransform`) e dispostas sequencialmente no mesmo arquivo.
Em razão da semântica de vinculação de nomes da linguagem Python, a última
definição sobrescreve as anteriores no espaço de nomes do módulo. Como a
definição final corresponde ao nó `limpa_2023_2024`, as três invocações
executam a mesma função e, por conseguinte, **as três edições recebem o
valor `ano_pesquisa = "2023_2024"`** nos arquivos Parquet da camada Silver.

O defeito não produz efeito sobre os resultados obtidos, uma vez que ambos
os jobs de unificação sobrescrevem o referido atributo a partir do nome da
partição em processamento, conforme a Listagem 2.

**Listagem 2 - Atribuição do ano a partir da partição**

```python
df = df.withColumn("ano_pesquisa", F.lit(edicao))
```

A fonte de verdade do ano de referência é, portanto, a estrutura de
partições do Amazon S3, e não o atributo homônimo gravado na camada Silver.
Registra-se, contudo, que a leitura direta de `silver/state_of_data/` sem a
devida atribuição conduziria à conclusão equivocada de que a totalidade dos
registros pertence à edição de 2023.

A correção consiste em atribuir identificadores distintos às três funções
(`MyTransform_2023`, `MyTransform_2024`, `MyTransform_2025`) e ajustar as
invocações correspondentes.

### 5.6 Regras de qualidade de dados

O script contém a definição `DEFAULT_DATA_QUALITY_RULESET`, composta por uma
única regra (`ColumnCount > 0`). Trata-se do conjunto padrão inserido
automaticamente pelo AWS Glue quando a funcionalidade de Data Quality
encontra-se habilitada. Sua capacidade de validação é nula para os fins
deste projeto, e sua presença decorre exclusivamente da configuração padrão
da ferramenta.

---

## 6. Artefato 2: `padronizacao_silver.py`

### 6.1 Caracterização

Implementação inicial da etapa de unificação. Entrega nove atributos,
suficientes para responder às questões relativas a perfil demográfico,
remuneração e diversidade de gênero.

### 6.2 Estratégia de identificação de atributos

A identificação apoia-se em dicionários estáticos, elaborados manualmente,
um para cada edição, conforme a Listagem 3.

**Listagem 3 - Dicionário estático de mapeamento (excerto)**

```python
MAPA_COLUNAS = {
    "2023_2024": {"p2_r_atualmente_qual_a_sua_forma_de_trabalho": "forma_trabalho", ...},
    "2024_2025": {"2_r_modelo_de_trabalho_atual": "forma_trabalho", ...},
    "2025_2026": {"2_q_modelo_de_trabalho_atual": "forma_trabalho", ...},
}
```

Observa-se, na terceira entrada, a manifestação do padrão III descrito na
Seção 4.2. O próprio código registra a anomalia em comentário
(`ATENCAO: em 2025_2026 a letra mudou de r para q`), o que constituiu o
indício inicial da limitação estrutural desta abordagem.

### 6.3 Padronização semântica

O artefato executa as seguintes operações de padronização:

- **Normalização de domínios categóricos** mediante `df.replace(mapa, subset=[coluna])`,
  a partir dos dicionários `GENERO`, `REGIAO`, `SENIORIDADE`,
  `FAIXA_SALARIAL` e `CARGO`, derivados da planilha
  `Mapa de Categorias - State of Data.xlsx` elaborada pelo grupo.

- **Derivação do atributo `faixa_salarial_ordem`**, com valores de 1 a 13 e
  o código 99 para ausência de resposta. Tal atributo é necessário à
  ordenação correta dos eixos gráficos, que de outro modo seguiriam a ordem
  lexicográfica dos rótulos.

- **Tratamento da forma de trabalho por correspondência parcial**
  (`contains`) em vez de igualdade estrita. A adoção desta estratégia
  decorre do fato de uma das alternativas de resposta consistir em
  enunciado extenso, cuja transcrição integral no editor do AWS Glue se
  mostrou inviável.

- **Substituição de valores nulos pelo literal `nao_informado`**, de modo a
  preservar a categoria nas representações gráficas.

- **Conversão da unidade federativa para caixa baixa.**

### 6.4 Tratamento de inconsistências da fonte

O dicionário de faixas salariais contempla duas entradas adicionais,
destinadas a acomodar erros tipográficos presentes no próprio instrumento
de coleta, conforme a Listagem 4.

**Listagem 4 - Entradas de correção de erros tipográficos da fonte**

```python
"de R$ 101/mês a R$ 2.000/mês": "1001_2000",     # omissão do algarismo 1
"de R$ 25.001/mês a R$ 3000/mês": "25001_30000", # omissão do separador de milhar
```

### 6.5 Esquema de saída

Destino: `silver_final/state_of_data_unificado/`, particionado por
`ano_pesquisa`.

Atributos: `ano_pesquisa`, `genero`, `uf`, `regiao`, `cargo`, `senioridade`,
`faixa_salarial`, `faixa_salarial_ordem`, `forma_trabalho`.

### 6.6 Limitações que motivaram a substituição

A substituição desta implementação fundamentou-se em três limitações:

**(i) Insuficiência de cobertura.** Nove atributos não permitem responder às
sete perguntas do enunciado. Não contemplam os domínios de tecnologia,
inteligência artificial, setor de atuação, tempo de experiência e porte da
organização.

**(ii) Fragilidade da identificação estática.** Cada nova edição exige a
revisão manual dos três dicionários, atributo por atributo. A natureza
sistemática do padrão III torna previsível a ocorrência de omissão em tal
procedimento.

**(iii) Ausência de falha explícita.** Não localizado o atributo, o script
emite advertência em log e cria a coluna com valores nulos. O job encerra
com status de sucesso e a base resultante apresenta atributo integralmente
vazio, sem sinalização que induza à verificação. Trata-se, portanto, de
falha silenciosa, condição inadequada a um processo de produção.

---

## 7. Artefato 3: `job_silver_unificada_v2.py`

### 7.1 Síntese comparativa

O Quadro 4 sintetiza as diferenças entre as duas implementações da etapa de unificação.

**Quadro 4 - Comparação entre as implementações**

| Critério | Versão 1 | Versão 2 |
|---|---|---|
| Atributos na saída | 9 | 64 |
| Identificação de atributo | nome estático | expressão regular sobre o nome |
| Registro em log | advertência de ausência | mapeamento origem-destino completo |
| Campos de múltipla escolha | não tratados | convertidos em atributos booleanos |
| Cobertura de não resposta | não mensurada | relatório por atributo e por ano |

### 7.2 Identificação por expressão regular

O núcleo do artefato consiste na estrutura `PADROES`, composta por 32
entradas no formato `(identificador_canônico, [expressão, expressão, ...])`,
conforme a Listagem 5.

**Listagem 5 - Estrutura de padrões de identificação (excerto)**

```python
PADROES = [
    ("faixa_idade",          [r"faixa_idade$", r"faixa_de_idade$"]),
    ("area_formacao",        [r"rea_de_forma", r"area_formacao$"]),
    ("forma_trabalho_ideal", [r"(forma|modelo)_de_trabalho_ideal", ...]),
    ("forma_trabalho",       [r"(forma|modelo)_de_trabalho_atual$", ...]),
]
```

A função `resolver_colunas()` percorre as expressões na ordem declarada e
retém a primeira correspondência obtida, removendo o atributo selecionado do
conjunto de candidatos, de modo a impedir sua reutilização. Havendo múltiplas
correspondências para a mesma expressão, adota-se o identificador de menor
comprimento, critério que privilegia a questão principal em detrimento de
seus desdobramentos.

Três condicionantes devem ser observadas:

**(a) A ordem de declaração é significativa.** A entrada
`forma_trabalho_ideal` deve preceder `forma_trabalho`, sob pena de a
segunda apropriar-se do atributo correspondente à primeira.

**(b) As expressões não devem conter caracteres acentuados.** Conforme
demonstrado na Seção 5.3, o artefato 1 converte acentos em sublinhados.
Por essa razão, `area_formacao` é identificado pela expressão
`rea_de_forma`, e não por sua grafia acentuada.

**(c) O log registra o mapeamento completo.** Cada execução imprime a
correspondência entre identificador de origem e identificador canônico,
acompanhada do sumário `resolvidas X de 32 colunas`. Constitui o primeiro
item de verificação após a execução do job.

### 7.3 Atributos derivados

**Atributos de ordenação e rotulação.** Os atributos
`faixa_salarial_ordem` e `num_funcionarios_ordem` destinam-se à ordenação
dos eixos; `faixa_salarial_label` e `num_funcionarios_label` contêm a
representação textual legível (`R$ 1.001 a R$ 2.000` em substituição a
`1001_2000`).

**Atributos booleanos.** Os campos de múltipla escolha armazenam diversas
respostas em um único valor textual, separadas por vírgula, condição que
inviabiliza o agrupamento em SQL. A estrutura `FLAGS` deriva 18 atributos
com domínio `sim`, `nao` e `nao_informado`, distribuídos em quatro de
linguagem, três de provedor de nuvem, quatro de ferramenta de BI e sete de
banco de dados. Adicionalmente, cinco atributos `motivo_*` são derivados do
campo `motivos_nao_ia`. A Listagem 6 reproduz um excerto da estrutura.

**Listagem 6 - Estrutura de derivação de atributos booleanos (excerto)**

```python
FLAGS = {
    "usa_python":     ("linguagens", r"(?i)python"),
    "usa_r":          ("linguagens", r"(?i)(^|,\s*)r\s*(,|$)"),
    "bi_powerbi":     ("bi", r"(?i)power\s*bi"),
    "banco_postgres": ("banco", r"(?i)postgre"),
}
```

A expressão associada a `usa_r` exige que o caractere `r` esteja delimitado
por vírgulas ou pelos limites da cadeia. A ausência de tal delimitação
produziria correspondência com qualquer termo que contivesse a letra.

**Estrutura `FLAGS_DUMMY`.** A partir da edição de 2025, o instrumento de
coleta substituiu o atributo textual único por um atributo por alternativa
(`4_c_1_sql`, `4_c_3_python`), configurando o padrão IV do Quadro 2. Quando
o atributo textual não se encontra presente na edição, os atributos
booleanos são derivados a partir destas colunas indicadoras.

**Atributos relativos a inteligência artificial.** Os campos `uso_ia` e
`ia_prioridade` apresentavam-se originalmente como enunciados extensos,
inadequados à rotulação de eixos gráficos. Foram convertidos em categorias
sintéticas, preservando-se o conteúdo original nos atributos
`uso_ia_texto` e `ia_prioridade_texto`, destinados exclusivamente à
rastreabilidade da categorização. **Os atributos com sufixo `_texto` não
devem compor representações gráficas.**

A ordem das cláusulas condicionais aplicadas a `uso_ia` é significativa,
dado que o campo admite múltipla escolha e uma mesma resposta pode conter
mais de um termo. A precedência adotada é: `pago_empresa`, `pago_proprio`,
`codigo`, `gratuito`, `nao_usa`.

### 7.4 Ordem de aplicação das transformações

A normalização textual (*slug*) precede a aplicação dos dicionários de
domínio, cujas chaves são convertidas ao mesmo formato no momento da
substituição, conforme a Listagem 7.

**Listagem 7 - Ordem de normalização e substituição**

```python
for c in COLUNAS_SLUG:
    df = df.withColumn(c, slug(c))

for coluna, mv in MAPA_VALORES.items():
    df = df.replace({_sl(k): v for k, v in mv.items()}, subset=[coluna])
```

Registra-se que a ordem inversa, adotada em versão anterior, produziu falha
relevante. A chave `"de R$ 1.001/mes a R$ 2.000/mes"`, grafada sem acento,
não estabelecia correspondência com o valor efetivamente presente na base,
que contém o termo `mês` acentuado. Como consequência, o atributo
`faixa_salarial` permaneceu sem tratamento e `faixa_salarial_ordem` assumiu
o valor 99 na totalidade dos registros. Os atributos `cargo` e `genero`
foram afetados de forma análoga, porém menos evidente. **A falha encontra-se
corrigida na versão em produção.**

### 7.5 Filtro de leitura da origem

A leitura da camada Silver aplica o filtro reproduzido na Listagem 8.

**Listagem 8 - Filtro aplicado à leitura da camada Silver**

```python
df = (spark.read
      .option("pathGlobFilter", "run-*.parquet")
      .parquet(caminho))
```

Cada pasta de partição contém um arquivo `versao_cloudShell.parquet`,
resultante de cópia manual dos mesmos dados. Na ausência do filtro, o Apache
Spark processa a totalidade dos arquivos da pasta e cada registro é
computado em duplicidade. A omissão desta configuração produziu contagens de
10.434 e 6.990 registros em execução anterior, correspondentes ao dobro dos
valores esperados.

### 7.6 Relatório de cobertura

Cinco atributos apresentam índice de não resposta superior a 80 por cento,
por terem sido submetidos exclusivamente a respondentes em posição de
gestão: `num_pessoas_dados`, `cargos_time`, `ia_prioridade`, `uso_ia` e
`motivos_nao_ia`.

Tal condição não configura falha do processo de tratamento, mas
característica do instrumento de coleta. Todo percentual calculado sobre
estes atributos possui validade restrita ao subconjunto de respondentes
efetivos, restrição que deve constar explicitamente na apresentação dos
resultados.

O relatório é persistido em `silver_final/relatorio_cobertura/` e contém o
percentual de não resposta por atributo e por edição.

**Nota de implementação.** O relatório é construído em passagem única
mediante a função `stack()`. A implementação inicial realizava uma agregação
por atributo, consolidadas por meio de operações `union` sucessivas. Tal
abordagem gerou plano de execução de dimensão excessiva e ocasionou a
interrupção do job por `OUT_OF_MEMORY_ERROR` no processo *driver*.

### 7.7 Decisões de desempenho

As seguintes decisões foram adotadas deliberadamente e não devem ser
revertidas sem análise prévia:

- **Ausência de `df.count()` na função `preparar()`.** A operação de contagem
  força a leitura integral da edição com a finalidade exclusiva de registrar
  um valor em log, o que triplica o tempo total de execução.

- **Persistência do Parquet anterior à geração do relatório.** Assegura que
  eventual falha na etapa de relatório não implique perda do processamento
  já concluído.

- **Releitura da base a partir do Amazon S3 para o relatório**, em
  substituição ao reaproveitamento do *DataFrame* em memória. O
  procedimento interrompe a linhagem de transformações e mantém o plano de
  execução em dimensão gerenciável.

---

## 8. Artefato 4: `TechChallenge_Notebook_Unificado_v4.ipynb`

### 8.1 Caracterização

Notebook Jupyter responsável pela camada Gold. Realiza a leitura da base
unificada, executa as agregações analíticas e produz os insumos que
respondem às sete perguntas de negócio do enunciado.

O artefato resulta da consolidação de três notebooks originalmente
independentes, elaborados por integrantes distintos do grupo. A unificação
preservou a divisão temática em três partes, que passaram a compartilhar uma
única sessão Spark e uma única leitura da camada Silver, definidas no
cabeçalho do arquivo.

Composição: 139 células, das quais 70 de código.

### 8.2 Pré-requisitos de ambiente

- Python 3 com Jupyter ou JupyterLab
- Java Runtime Environment, requisito do Apache Spark
- Acesso ao AWS Academy (Learner Lab)

As dependências são instaladas pela primeira célula de código: `pyspark`,
`pandas`, `matplotlib`, `seaborn`, `numpy`, `ipykernel`, `pyarrow` e `s3fs`.

### 8.3 Configuração de credencial

O conjunto de dados encontra-se em conta AWS do Learner Lab. A execução com
credenciais de outra conta resulta em erro de autorização (`Access Denied`,
HTTP 403).

Procedimento de configuração:

1. No AWS Academy, acionar **Start Lab** e aguardar a confirmação de
   disponibilidade do ambiente.
2. Acessar **AWS Details** e, na linha *AWS CLI*, acionar **Show**.
3. Copiar o bloco de credenciais e inseri-lo no arquivo
   `~/.aws/credentials`, substituindo o rótulo `[default]` por `[labs]`,
   de modo a preservar eventuais perfis de conta pessoal.
4. Validar no terminal:

```bash
aws sts get-caller-identity --profile labs
```

**O notebook não define o perfil AWS internamente.** É necessário exportar a
variável de ambiente antes de iniciar o Jupyter, conforme a Listagem 9, ou
declarar o perfil `labs` como padrão na configuração da AWS CLI.

**Listagem 9 - Definição do perfil AWS previamente à execução**

```bash
export AWS_PROFILE=labs
jupyter lab
```

**Advertência.** A credencial do Learner Lab é temporária, identificada pelo
prefixo `ASIA`, e expira decorridas algumas horas. É igualmente renovada a
cada acionamento de **Start Lab**. A ocorrência de erro `InvalidClientTokenId`
na chamada ao AWS STS, ou de erro HTTP 400 em operação sobre o Amazon S3,
indica credencial expirada e exige a repetição do procedimento acima.
Após a substituição da credencial, é necessário **reiniciar o kernel do
Jupyter**, uma vez que a biblioteca `pyarrow` mantém a credencial em cache
no processo em execução.

### 8.4 Estratégia de leitura da camada Silver

A leitura é realizada por meio das bibliotecas `pandas` e `s3fs`, e não
diretamente pelo Apache Spark, conforme a Listagem 10.

**Listagem 10 - Carga da camada Silver**

```python
_pdf_silver = pd.read_parquet(CAMINHO_SILVER)

for _c in _pdf_silver.columns:
    if str(_pdf_silver[_c].dtype) == "category":
        _pdf_silver[_c] = _pdf_silver[_c].astype(str)

def carregar_silver():
    return spark.createDataFrame(_pdf_silver)
```

A decisão fundamenta-se em dois motivos. Primeiro, o Spark em execução local
requer os pacotes `hadoop-aws` e `aws-java-sdk` para acessar o protocolo
`s3a`, dependências cuja configuração se mostrou desnecessária ante o volume
processado. Segundo, o conjunto possui 14.005 registros, dimensão para a
qual a carga em memória e a conversão subsequente são instantâneas.

A conversão de tipo `category` para `str` é obrigatória: o atributo de
particionamento `ano_pesquisa` retorna do Parquet com aquele tipo, não
suportado pelo construtor `createDataFrame`.

### 8.5 Estrutura analítica

O notebook organiza-se em três partes, correspondentes às sete perguntas do
enunciado, conforme o Quadro 5.

**Quadro 5 - Estrutura analítica do notebook**

| Parte | Pergunta | Objeto de análise |
|:---:|:---:|---|
| 1 | 1 | Estrutura do mercado brasileiro de dados: cargo, setor e situação de trabalho |
| 1 | 2 | Perfis profissionais mais valorizados: remuneração por cargo, ranking salarial e remuneração por senioridade |
| 1 | 3 | Diversidade de gênero: distribuição geral, por cargo e por senioridade |
| 2 | 4 | Adoção de tecnologias: linguagens, nuvem, BI e bancos de dados, em série temporal das três edições |
| 2 | 5 | Adoção de inteligência artificial: índice geral, forma de uso e prioridade estratégica nas organizações |
| 3 | 6 | Assimetrias por região, senioridade e modelo de trabalho, incluindo distribuição, remuneração e uso de IA |
| 3 | 7 | Oportunidades e desafios: prioridade de IA entre gestores, motivos de não adoção e setores que mais priorizam a tecnologia |

Cada pergunta é acompanhada de síntese executiva em células de marcação, e o
notebook encerra com conclusão geral.

### 8.6 Convenções de exportação

As saídas são persistidas no diretório `Gold_output/`, criado no diretório de
trabalho corrente, conforme o Quadro 6.

**Quadro 6 - Artefatos persistidos pela camada Gold**

| Artefato | Pergunta |
|---|:---:|
| `tecnologias_adocao.csv` | 4 |
| `grafico_tecnologias_adocao.png` | 4 |
| `grafico_linguagens_ranking.png` | 4 |
| `grafico_comparativo_2023_2025.png` | 4 |
| `heatmap_panorama_tecnologias.png` | 4 |
| `grafico_ia_adocao_geral.png` | 5 |
| `grafico_ia_tipo_uso.png` | 5 |
| `grafico_ia_prioridade.png` | 5 |
| `grafico_evolucao_foco.png` | 5 |

A publicação no Amazon S3 é realizada conforme a Listagem 11.

**Listagem 11 - Publicação da camada Gold**

```bash
aws s3 cp Gold_output/ s3://tc-fase3-state-of-data-seu-id/gold/ \
    --recursive --profile labs
```

**Observação quanto à cobertura da exportação.** As análises das sete
perguntas são integralmente executadas no notebook e seus resultados
apresentados em tela. A exportação automática para arquivo, contudo,
contempla apenas as perguntas 4 e 5. Os resultados das demais perguntas
constam das células de saída do próprio notebook e das sínteses executivas
que as acompanham, tendo sido igualmente incorporados ao relatório executivo
e à apresentação do grupo.
## 9. Resultados e validação

### 9.1 Volumetria obtida

A volumetria da base unificada consta do Quadro 7.

**Quadro 7 - Registros por edição na base unificada**

| Edição | Registros |
|---|---:|
| 2023_2024 | 5.293 |
| 2024_2025 | 5.217 |
| 2025_2026 | 3.495 |
| **Total** | **14.005** |

Esquema resultante: 63 atributos, acrescidos do atributo de particionamento
`ano_pesquisa`.

### 9.2 Validação da camada Silver

A validação da volumetria foi conduzida por dupla verificação independente.
Os totais obtidos pelo pipeline em ambiente AWS foram confrontados com os
resultados de consolidação executada em paralelo por outro integrante do
grupo, em ambiente distinto e sem utilização dos serviços AWS. Ambas as
construções convergiram para o mesmo total de 14.005 registros, o que
confere confiabilidade ao resultado.

Validação complementar foi realizada no Amazon Athena por meio da consulta
apresentada na Listagem 12.

**Listagem 12 - Consulta de validação de volumetria**

```sql
SELECT ano_pesquisa, COUNT(*) AS linhas
FROM db_tc_fase3.state_of_data_unificado
GROUP BY ano_pesquisa
ORDER BY ano_pesquisa;
```

### 9.3 Validação da camada Gold

A verificação do notebook foi conduzida mediante execução integral contra
base sintética construída com o esquema e os domínios de valor idênticos aos
da camada Silver, procedimento que permite exercitar a totalidade das
células sem dependência de credencial ativa. As 70 células de código
executaram-se sem interrupções nem advertências, com persistência dos nove
artefatos relacionados no Quadro 6.

---

## 10. Limitações conhecidas

**10.1 Atributo `ano_pesquisa` na camada Silver intermediária.** Conforme
descrito na Seção 5.5, o valor gravado é incorreto para duas das três
edições. A limitação não afeta os resultados, porém condiciona a leitura
direta da camada Silver.

**10.2 Índice de não resposta em atributos restritos a gestores.** Conforme
a Seção 7.6, cinco atributos apresentam cobertura inferior a 20 por cento.
Percentuais calculados sobre tais atributos possuem validade restrita ao
subconjunto de respondentes efetivos.

**10.3 Ausência de cargo declarado.** Aproximadamente 27 por cento dos
respondentes não informaram cargo, situação atribuível, em parte, a
respondentes não atuantes na área de dados. Análises restritas a
profissionais de dados devem aplicar o filtro `WHERE cargo <> 'nao_informado'`,
e o critério adotado deve ser explicitado.

**10.4 Preservação de registros incompletos.** A camada Silver preserva a
totalidade dos registros, atribuindo o literal `nao_informado` às respostas
ausentes. A decisão fundamenta-se no fato de que o critério de exclusão
depende da questão analisada: constam 1.161 respondentes sem faixa salarial
declarada, cuja remoção na camada Silver comprometeria as análises de
gênero, região e ferramentas, para as quais tais registros são válidos. O
filtro deve, portanto, ser aplicado no âmbito de cada análise específica.

**10.5 Escopo das perguntas 6 e 7.** As análises correspondentes a estas
questões incidem sobre a totalidade dos 14.005 respondentes, e não
exclusivamente sobre aqueles que declararam cargo e senioridade. A opção
encontra-se assinalada no próprio notebook. Sua alteração modifica a
totalidade dos indicadores subsequentes e implica divergência em relação aos
valores constantes do relatório executivo e da apresentação.

**10.6 Células remanescentes da consolidação.** Subsistem no notebook
células oriundas dos três arquivos originais, atualmente inócuas
(reinstalação de dependências e redefinição da sessão Spark), bem como
trechos comentados referentes a execução anterior em AWS Glue, preservados
como registro histórico.

---

## 11. Procedimento de reexecução

### 11.1 Camadas Bronze, Silver e Silver final

1. Acessar o AWS Academy e acionar **Start Lab**, aguardando a confirmação
   de disponibilidade do ambiente.
2. No AWS Glue, seção *ETL jobs*, executar o job correspondente ao
   `ETL.py` (Bronze para Silver).
3. No AWS Glue, seção *ETL jobs*, selecionar `job-silver-unificada-v2`.
4. Em *Job details*, confirmar: função IAM `LabRole` e **Job bookmark em
   `Disable`**.
5. Executar (*Save* seguido de *Run*). Tempo médio de execução: 4 minutos.
6. Consultar *Output logs* e verificar o sumário `resolvidas X de 32 colunas`.
7. Executar o crawler `crawler-silver-final` e aguardar o status *Ready*.
8. Validar no Amazon Athena mediante a consulta da Listagem 12.

**Advertência quanto ao Job bookmark.** O parâmetro deve permanecer em
`Disable`. Quando habilitado, o AWS Glue registra os arquivos previamente
processados, omite-os na execução subsequente e persiste saída vazia sem
emissão de erro, configurando falha silenciosa.

Na hipótese de reinicialização do ambiente com perda do job, procede-se à
sua recriação por meio do *Script editor*, com a inserção do conteúdo de
`job_silver_unificada_v2.py`.

### 11.2 Camada Gold

9. Configurar a credencial do Learner Lab conforme a Seção 8.3.
10. Abrir o notebook em ambiente Jupyter e reiniciar o kernel.
11. Executar as células **em ordem sequencial**, a partir da seção de
    configuração. As partes 2 e 3 reaproveitam a sessão Spark, a função
    `carregar_silver()` e os diretórios de saída definidos no cabeçalho.
12. Verificar o conteúdo do diretório `Gold_output/`, conforme o Quadro 6.
13. Publicar a camada Gold no Amazon S3 conforme a Listagem 11.

---

## 12. Diagnóstico de falhas

O Quadro 8 relaciona os sintomas observados durante o desenvolvimento às respectivas causas e aos pontos de verificação correspondentes.

**Quadro 8 - Sintomas, causas prováveis e pontos de verificação**

| Sintoma observado | Causa provável | Ponto de verificação |
|---|---|---|
| Atributo integralmente preenchido com `nao_informado` | Expressão regular sem correspondência | Bloco `NAO ENCONTRADAS` no log de execução |
| Expressão regular acentuada sem correspondência | Conversão de acento em sublinhado pelo `ETL.py` | Estrutura `PADROES` |
| Contagem de registros duplicada | Presença de `versao_cloudShell.parquet` na partição | Parâmetro `pathGlobFilter` |
| Saída vazia com status de sucesso | Job bookmark habilitado | Seção *Job details* |
| `OUT_OF_MEMORY_ERROR` no processo driver | Plano de execução de dimensão excessiva | Ocorrências de `union` em laço e de `count()` |
| Atributo incorretamente identificado | Expressão regular insuficientemente restritiva | Linha de mapeamento no log de execução |
| Base aparentemente restrita a 2023 | Definições homônimas de `MyTransform` no `ETL.py` | Seção 5.5 deste documento |
| `InvalidClientTokenId` ou HTTP 400 no Amazon S3 | Credencial temporária expirada | Seção 8.3 deste documento |
| `Access Denied` (HTTP 403) no Amazon S3 | Perfil apontando para conta AWS diversa da do Learner Lab | `aws sts get-caller-identity --profile labs` |
| Erro de acesso persistente após renovação da credencial | Credencial mantida em cache pelo `pyarrow` | Reiniciar o kernel do Jupyter |
| Diretório de saída inexistente ao publicar | Terminal posicionado em diretório distinto do notebook | Diretório de trabalho corrente |

---

## 13. Procedimento para inclusão de novos atributos

A inclusão de atributo não deve ser realizada localmente por integrante
individual do grupo, sob pena de as análises deixarem de ser comparáveis
entre si. Pelo mesmo motivo, a leitura da camada Silver não deve ser
substituída por arquivo local.

O procedimento estabelecido consiste em comunicar a necessidade ao grupo, de
modo que a alteração seja incorporada ao job e a base resultante permaneça
única para todos os integrantes.

Operacionalmente, a inclusão de um atributo requer a adição de uma entrada
na estrutura `PADROES` e a reexecução do pipeline, conforme o procedimento
da Seção 11.
