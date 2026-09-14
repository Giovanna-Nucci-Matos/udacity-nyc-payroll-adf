# NYC Payroll Data Integration — Azure Data Factory

Pipeline de integração de dados construído para a plataforma de análise da folha de pagamento da
cidade de Nova York. Os arquivos de master data e de folha de pagamento chegam no **Azure Data Lake
Storage Gen2**, são carregados no **Azure SQL Database** por Mapping Data Flows, agregados por
agência e ano fiscal, e gravados tanto na tabela de resumo do SQL quanto no diretório `dirstaging`
do Data Lake, exposto ao **Azure Synapse Analytics** por meio de uma external table.

```
ADLS Gen2 (dirpayrollfiles, dirhistoryfiles)
        |  carga (Mapping Data Flows)
        v
Azure SQL DB (db_nycpayroll)
        |  união + filtro + coluna derivada + agregação
        v
+-> dbo.NYC_Payroll_Summary (Azure SQL DB)
+-> ADLS Gen2 /dirstaging  -->  external table dbo.NYC_Payroll_Summary (Synapse serverless)
```

## Recursos do Azure

| Recurso | Nome |
|---|---|
| Data Lake Storage Gen2 | `adlsnycpayrollgiovannan` |
| Contêiner | `adlsnycpayroll-giovanna-n` |
| Diretórios | `dirpayrollfiles`, `dirhistoryfiles`, `dirstaging` |
| Azure SQL Database | `db_nycpayroll` (servidor `sqlnycpayrollgiovannan`, camada Basic) |
| Azure Data Factory | `adf-nycpayroll-giovanna` |
| Synapse Analytics | `syn-nycpayroll-giovanna-n`, banco `udacity`, pool SQL serverless |

Arquivos de origem: `EmpMaster.csv`, `AgencyMaster.csv`, `TitleMaster.csv` e `nycpayroll_2021.csv`
em `dirpayrollfiles`; `nycpayroll_2020.csv` em `dirhistoryfiles`.

## Estrutura do repositório

| Pasta | Conteúdo |
|---|---|
| `factory/` | Definição do Data Factory com o parâmetro global `dataflow_param_fiscalyear` |
| `linkedService/` | Conexões com o ADLS Gen2 e o Azure SQL Database |
| `dataset/` | Datasets de texto delimitado sobre o Data Lake e de tabela sobre o SQL DB |
| `dataflow/` | Cinco fluxos de carga e o fluxo de agregação |
| `pipeline/` | Pipeline de orquestração com seis atividades `ExecuteDataFlow` |
| `screenshots/` | Evidências de execução |

## Serviços vinculados

| Nome | Tipo | Observação |
|---|---|---|
| `ls_adls_nycpayroll` | `AzureBlobFS` | Data Lake Gen2 com os arquivos de origem |
| `ls_sqldb_nycpayroll` | `AzureSqlDatabase` | `db_nycpayroll`, versão 1.0 (Legacy), exigida pelos data flows |

## Datasets

Seis sobre o Data Lake (`AzureBlobFSLocation`): um por arquivo CSV mais o diretório de staging, que
grava sem cabeçalho para casar com o formato lido pela external table do Synapse.

Seis sobre o SQL DB (`AzureSqlTable`): `NYC_Payroll_AGENCY_MD`, `NYC_Payroll_EMP_MD`,
`NYC_Payroll_TITLE_MD`, `NYC_Payroll_Data_2020`, `NYC_Payroll_Data_2021` e `NYC_Payroll_Summary`.

## Fluxos de dados

Cinco fluxos de carga movem cada arquivo do Data Lake para a tabela correspondente no SQL DB,
truncando o destino antes da inserção para que o pipeline possa ser reexecutado:
`df_agency_md`, `df_emp_md`, `df_title_md`, `df_payroll_2020` e `df_payroll_2021`.

O `df_summary` faz a agregação:

1. `srcSql2020` e `srcSql2021` — tabelas de folha de pagamento no Azure SQL DB
2. `selPayroll2020` e `selPayroll2021` — alinham os dois esquemas; o de 2021 mapeia `AgencyCode` para `AgencyID`
3. `unionPayroll` — união por nome
4. `filterFiscalYear` — `toInteger(FiscalYear) >= $dataflow_param_fiscalyear`
5. `deriveTotalPaid` — `TotalPaid = RegularGrossPaid + TotalOTPaid + TotalOtherPay`
6. `aggSummary` — agrupa por `AgencyName` e `FiscalYear`, com `sum(TotalPaid)`
7. `selSummaryOutput` — ordena as colunas como `FiscalYear, AgencyName, TotalPaid`, que é a ordem lida pela external table
8. `sinkSqlSummary` — grava em `dbo.NYC_Payroll_Summary` com truncate
9. `sinkStagingSummary` — grava em `dirstaging` em arquivo único, limpando a pasta antes

## Pipeline

`pl_nyc_payroll_summary` executa os três fluxos de master data em paralelo, depois os dois de folha
de pagamento, e por fim a agregação:

```
dataflow_agency  --+
dataflow_emp     --+--> dataflow_payroll2020 --+
dataflow_title   --+--> dataflow_payroll2021 --+--> dataflow_summary
```

O parâmetro global `dataflow_param_fiscalyear` (valor 2020) é passado ao parâmetro do fluxo por
`@pipeline().globalParameters.dataflow_param_fiscalyear`, o que permite mudar a janela de agregação
sem editar o data flow.

## Resultado da execução

| Tabela | Linhas |
|---|---|
| `NYC_Payroll_AGENCY_MD` | 153 |
| `NYC_Payroll_EMP_MD` | 1000 |
| `NYC_Payroll_TITLE_MD` | 1446 |
| `NYC_Payroll_Data_2020` | 100 |
| `NYC_Payroll_Data_2021` | 101 |
| `NYC_Payroll_Summary` | 25 |

O arquivo `nyc_payroll_summary.csv` é gerado em `dirstaging` e consultado pela external table do
Synapse, retornando os totais por agência para os anos fiscais de 2020 e 2021.

## Evidências

| Arquivo | Conteúdo |
|---|---|
| `01-datalake-arquivos.png` | Arquivos CSV carregados no Data Lake |
| `02-tabelas-sqldb.png` | Tabelas criadas no `db_nycpayroll` |
| `03-external-table-synapse.png` | External table criada no Synapse |
| `04-linked-services.png` | Serviços vinculados no Data Factory |
| `05-datasets.png` | Datasets no Data Factory |
| `06-dataflows-carga.png` | Fluxos de dados de carga |
| `07-dataflow-summary.png` | Fluxo de agregação |
| `08-pipeline.png` | Pipeline de orquestração |
| `09-pipeline-run-sucesso.png` | Execução bem-sucedida com as seis atividades |
| `10-query-sqldb-summary.png` | Consulta na tabela de resumo do SQL DB |
| `11-dirstaging-arquivos.png` | Arquivo gerado em `dirstaging` |
| `12-query-synapse-summary.png` | Consulta na external table do Synapse |
