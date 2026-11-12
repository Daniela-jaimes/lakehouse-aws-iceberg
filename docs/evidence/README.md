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

19 SQL (mini): Q1 con resultado, tiempo y Data scanned
![19-q1](./images/19-Q1_mini.png)

20 SQL (mini): Q2 con resultado, tiempo y Data scanned
![20-q2](./images/20-Q2_mini.png)

21 SQL (mini): Q3 con resultado, tiempo y Data scanned
![21-q3](./images/21-Q3_mini.png)

22 SQL: $snapshots
![22-timetravel](./images/22-snapchots.png)

22.1 SQL: timetravel
![22-timetravel](./images/22.1-timetravel.png)

22.2 SQL: current_count
![22-timetravel](./images/22.2-current_count.png)

23 SQL: UPDATE y DELETE con verificación en 0
![23-update-delete](./images/23-update_delete.png)

24 Full: fact_sales ≥ 10M y total entre capas
![24-volumen](./images/24-fact-sales.png)

25 Full: duración y DPU-horas de la corrida full
![25-glue-full](./images/25-glue-full.png)

26 Full: integridad referencial en 0
![26-integridad](./images/26-integrity_0.png)

27.1 Costos: Cost Explorer 
![27-costos](./images/27-cost_explorer.png)

27.1 Costos: Cost glue
![27-costos](./images/27-cost_glue.png)

29 SQL (full): Q1 con resultado, tiempo y Data scanned
![29-q1](./images/29-Q1-full.png)

30 SQL (full): Q2 con resultado, tiempo y Data scanned
![30-Q2](./images/30-Q2-full.png)

31.1 SQL (full): Q3 sobre la estrella (con joins), con resultado, tiempo y Data scanned
![31.1-Q3](./images/31.1-Q3-full_joins.png)

31.2 SQL (full): Q3 sobre el data mart (sin joins), con resultado, tiempo y Data scanned
![31.2-Q3](./images/31.2-Q3-full_no_joins.png)

32 SQL (full): Q4 categorías con más devoluciones, con resultado, tiempo y Data scanned
![32-Q4](./images/33-Q4_full.png)

33 Cierre: terraform destroy y verificación
![33-destroy](./images/28-destroy.png)
