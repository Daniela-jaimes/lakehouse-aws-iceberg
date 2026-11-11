# Evidencias

# Aqui se pueden observar las evidencias de cada aspecto trabajado en el proyecto

1 Terraform Apply complete! Resources: 31 added 
![Terraform Apply complete! Resources: 31 added](./images/01-apply.png)

2 Terraform S3: bucket con sus prefijos 
![Terraform S3: bucket con sus prefijos](./images/02-s3.png)

3 Terraform Budgets: 4 alertas
![Terraform Budgets: 4 alertas](./images/budgets.png)

4 Terraform Athena: workgroup lakehouse-wg, engine v3
![Terraform Athena: workgroup lakehouse-wg](./images/04-athena.png)

5 Terraform Glue: 3 bases de datos
![Terraform Glue: 3 bases de datos](./images/05-gluedbs.png)

6 Terraform Glue: los 2 jobs
![Terraform Glue: los 2 jobs](./images/06-gluejobs.png)

7 Terraform Step Functions: lakehouse-pipeline
![Terraform Step Functions](./images/07-sfn.png)

8 Terraform IAM: 3 roles y usuario con MFA
![Terraform IAM: 3 roles y usuario con MFA](./images/8.jpeg)
 
9 S3 `landing/` con `ingest_date=...` 
![S3 `landing/` con `ingest_date=...`](./images/09-landing.png)

10 Crawlers en Succeeded 
![Crawlers en Succeeded](./images/10_crawlers_de_Bronze.png)

11 Tablas `raw_*` y conteo en Athena 
![Tablas `raw_*` y conteo en Athena ](./images/11-tablas_Bronze.png)

12 Run `bronze-to-silver` en Succeeded (duración, DPU-horas) 
![Run `bronze-to-silver` en Succeeded](./images/12-job_silver.png)

13 CloudWatch: líneas `[QUALITY]` 
![CloudWatch: líneas `[QUALITY]`](./images/13-quality-logs.png)

14 `silver/_rejects/` y 5 tablas de silver 
![`silver/_rejects/` y 5 tablas de silver](./images/14-AWS-tables.png)

14 `silver/_rejects/` y 5 tablas de silver 
![`silver/_rejects/` y 5 tablas de silver](./images/14-silver.png)

15 Run `silver-to-gold` en Succeeded 
![Run `silver-to-gold` en Succeeded](./images/15-job-gold.png)

16 7 tablas de gold en Glue 
![7 tablas de gold en Glue](./images/16-gold.png)

17 Step Functions: grafo en verde 
![TStep Functions: grafo en verde](./images/17-sfn-ok.png)

18 Conteos antes/después del lote 2  
![Conteos antes/después del lote 2  ](./images/18-merge.png)
