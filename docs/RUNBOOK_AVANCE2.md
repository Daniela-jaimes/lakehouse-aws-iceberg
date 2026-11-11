# Runbook del Avance 2 (de cero a las evidencias 9–18)

Se ejecuta desde la raíz del repo en **Git Bash**. Terraform ya está aplicado.
Los números esperados salen de simular las reglas de los jobs sobre los datos generados con el perfil `mini`.

```bash
export AWS_PROFILE=lakehouse
aws sts get-caller-identity        # must show lakehouse-admin
cd terraform && BUCKET=$(terraform output -raw bucket_name) && cd ..
```

## 1. Generar el lote 1 (gratis, local)

```bash
cd data
python generate_data.py --profile mini --batch 1 --ingest-date 2026-11-07
cd ..
```

Esperado: customers 5.043, products 500, stores 30, orders 50.518, order_items 124.873, web_events 20.000
(incluye duplicados intencionales).

## 2. Subir a landing y catalogar (evidencias 9, 10, 11)

```bash
aws s3 sync data/out/landing s3://$BUCKET/landing/
aws glue start-crawler --name lakehouse-bronze-oltp
aws glue start-crawler --name lakehouse-bronze-logs
aws glue get-crawler --name lakehouse-bronze-oltp --query Crawler.State     # repeat until READY
```

En Athena (workgroup `lakehouse-wg`):

```sql
SHOW TABLES IN lakehouse_bronze;
SELECT count(*) FROM lakehouse_bronze.raw_orders;           -- 50518
SELECT count(*) FROM lakehouse_bronze.raw_order_items;      -- 124873
DESCRIBE lakehouse_bronze.raw_orders;                       -- check the column names
```

Columnas que los jobs esperan: ver la sección 5 de la guía. Si `DESCRIBE` muestra otros nombres, avisar antes de seguir.

## 3. Correr el pipeline del lote 1 (evidencias 12–17)

```bash
SM=$(cd terraform && terraform output -raw state_machine_arn)
aws stepfunctions start-execution --state-machine-arn $SM --input '{"ingest_date":"2026-11-07"}'
```

Seguir la ejecución en Step Functions. Logs en CloudWatch → `/aws-glue/jobs/output`.

Esperado en los logs `[QUALITY]`: customers rejected=51 · orders rejected=150 · order_items rejected=590 · products y stores 0.

Esperado en Athena tras el lote 1:

| Tabla | Filas |
|---|---|
| silver.customers | 4.949 |
| silver.products / stores | 500 / 30 |
| silver.orders | 49.850 |
| silver.order_items | 122.984 |
| gold.fact_sales | 122.586 |
| gold.dim_customer / dim_product / dim_store | 4.950 / 501 / 31 |
| gold.dim_geography / dim_date | 9 / 4.019 |

(fact_sales tiene menos filas que order_items porque los pedidos con fecha futura se rechazan y sus líneas no entran al hecho.)

## 4. Lote 2 y evidencia del upsert (evidencia 18)

Primero anotar el estado previo en Athena:

```sql
SELECT count(*) FROM lakehouse_silver.orders;                            -- 49850
SELECT count(*) FROM lakehouse_silver.orders WHERE status = 'returned';  -- 2923
SELECT current_timestamp;                                                -- write it down (UTC)
```

```bash
cd data
python generate_data.py --profile mini --batch 2 --ingest-date 2026-11-09 --batch1-date 2026-11-07
cd ..
aws s3 sync data/out/landing s3://$BUCKET/landing/
```

En Athena, registrar las particiones nuevas:

```sql
MSCK REPAIR TABLE lakehouse_bronze.raw_customers;
MSCK REPAIR TABLE lakehouse_bronze.raw_products;
MSCK REPAIR TABLE lakehouse_bronze.raw_stores;
MSCK REPAIR TABLE lakehouse_bronze.raw_orders;
MSCK REPAIR TABLE lakehouse_bronze.raw_order_items;
MSCK REPAIR TABLE lakehouse_bronze.raw_web_events;
```

```bash
aws stepfunctions start-execution --state-machine-arn $SM --input '{"ingest_date":"2026-11-09"}'
```

Esperado después:

| Medida | Antes | Después |
|---|---|---|
| silver.orders | 49.850 | 53.850 (+4.000) |
| silver.orders con status = 'returned' | 2.923 | 3.923 (+1.000) |
| silver.order_items | 122.984 | 132.725 |
| gold.fact_sales | 122.586 | 132.327 |

Capturar antes y después en una sola imagen (`18-merge.png`). Con esto también se puede mostrar time travel
(`sql/05_lakehouse.sql`).

## 5. Si algo falla

| Error | Qué hacer |
|---|---|
| `AnalysisException: cannot resolve column` | El nombre de una columna de bronze no coincide: pegar el `DESCRIBE` y ajustar la función `prepare_*`. |
| `ModuleNotFoundError: helpers` | Revisar `aws s3 ls s3://$BUCKET/scripts/` (debe estar `helpers.zip`) y `--extra-py-files` en el job. |
| `ValueError: reject rate ... above 5 %` | Mirar `silver/_rejects/<entidad>/` y el motivo en `_reject_reason`. |
| Error de Iceberg al escribir | Probar `glue_version = "4.0"` en `terraform.tfvars` y reaplicar. |

Gasto: antes de cada corrida, el comando de la sección 2 de la guía (acumulado de Glue en USD).
