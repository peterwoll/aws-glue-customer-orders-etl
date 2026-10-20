# AWS Glue Customer Orders ETL Pipeline

**Status:** Completed and validated | **Region:** `us-east-1` | **Validation date:** October 20, 2026

## Project overview

Built an end-to-end AWS data engineering pipeline that extracts relational data from Amazon RDS for MySQL, transforms it with AWS Glue and PySpark, writes columnar Parquet output to Amazon S3, and queries the transformed data using Amazon Athena.

**Verified outcome:** Athena returned **8 customer-order records** with a combined **$2,159.93 in sales**.

## Architecture

```text
Amazon RDS for MySQL
  glue_etl_demo.customers + glue_etl_demo.orders
            |
            | JDBC connection (VPC networking)
            v
AWS Glue 5.1 / PySpark: customer-orders-etl
  - Inner join on customer_id
  - Combine first and last name
  - Calculate quantity * unit_price as order_total
            |
            | Parquet (Snappy)
            v
Amazon S3
  s3://glue-etl-lab-235494792645/processed/customer_orders/
            |
            v
Amazon Athena / AWS Glue Data Catalog
  glue_etl_analytics.customer_orders
            |
            v
  8 orders | $2,159.93 total sales
```

## AWS services and configuration

| Component | Configuration |
| --- | --- |
| Source | Amazon RDS for MySQL; database `glue_etl_demo`; tables `customers`, `orders` |
| ETL | AWS Glue Spark job `customer-orders-etl`; Glue 5.1; G.1X, 2 workers |
| Connection | AWS Glue JDBC connection `glue-etl-mysql-connection` |
| Secrets | AWS Secrets Manager secret `glue-etl/mysql-credentials` |
| IAM | Glue execution role `AWSGlueServiceRole` |
| Networking | VPC `mysql-lab-vpc`; private connectivity; S3 gateway endpoint and Secrets Manager interface endpoint |
| Storage | `s3://glue-etl-lab-235494792645/processed/customer_orders/` |
| Analytics | Athena database `glue_etl_analytics`, table `customer_orders` |
| Region | `us-east-1` |

## Source tables

**customers:** `customer_id`, `first_name`, `last_name`, `email`, `city`, `state`, `signup_date`.

**orders:** `order_id`, `customer_id`, `order_date`, `product_name`, `quantity`, `unit_price`.

## PySpark transformation (verified excerpt)

```python
from pyspark.sql.functions import col, concat_ws, round

joined = customers.join(
    orders,
    customers.customer_id == orders.customer_id,
    "inner"
)

result = joined.select(
    customers.customer_id.alias("customer_id"),
    concat_ws(" ", customers.first_name, customers.last_name).alias("customer_name"),
    customers.email,
    customers.city,
    customers.state,
    orders.order_id,
    orders.order_date,
    orders.product_name,
    orders.quantity,
    orders.unit_price,
    round(col("quantity") * col("unit_price"), 2).alias("order_total")
)

output_path = "s3://glue-etl-lab-235494792645/processed/customer_orders/"
result.write.mode("overwrite").parquet(output_path)
print("Customer orders ETL completed successfully")
```

> This is the verified transformation/output excerpt, not the full runnable Glue job. The complete job also includes imports, Glue/Spark initialization, and JDBC reads. Do not commit database credentials or connection secrets.

## Athena table and validation

The following table definition was used for the project; its schema was confirmed by Athena's table pane and successful aggregate query.

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS customer_orders (
    customer_id INT,
    customer_name STRING,
    email STRING,
    city STRING,
    state STRING,
    order_id INT,
    order_date DATE,
    product_name STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    order_total DECIMAL(13,2)
)
STORED AS PARQUET
LOCATION 's3://glue-etl-lab-235494792645/processed/customer_orders/';
```

```sql
SELECT * FROM glue_etl_analytics.customer_orders LIMIT 10;

SELECT
    COUNT(*) AS total_orders,
    ROUND(SUM(order_total), 2) AS total_sales
FROM glue_etl_analytics.customer_orders;
```

**Observed Athena results:**

| total_orders | total_sales |
| ---: | ---: |
| 8 | 2159.93 |

## Implementation milestones

1. Created and populated the RDS MySQL `customers` and `orders` tables.
2. Configured the Glue JDBC connection and tested access to MySQL.
3. Created a Secrets Manager secret and configured IAM permissions.
4. Set up VPC endpoints to support Glue access to S3 and Secrets Manager.
5. Created the Glue 5.1 PySpark job, joined the tables, and wrote Parquet output.
6. Resolved S3 permission issues and reran the Glue job successfully.
7. Confirmed the Parquet object existed in S3.
8. Registered the Parquet output in Athena and validated the record count and sales total.

## Troubleshooting and lessons learned

- **Secrets Manager AccessDenied:** Updated Glue role permissions to allow `secretsmanager:GetSecretValue` for the connection secret.
- **S3 AccessDenied on job output:** Corrected the Glue role's S3 policy to target the actual output bucket, `glue-etl-lab-235494792645`, rather than a different bucket name.
- **VPC access:** Configured S3 gateway and Secrets Manager interface endpoints for private-network service access.
- **Athena catalog organization:** Used a dedicated analytics database instead of an unrelated older `customers_parquet` table.

## Security and cost considerations

- Keep MySQL credentials in Secrets Manager; never hard-code or publish them.
- Grant the Glue role least-privilege access to the secret and required S3 prefixes.
- Limit RDS inbound access to authorized security groups.
- Review ongoing charges for RDS, Glue, VPC interface endpoints, S3, and Athena. Shut down or delete lab resources when no longer needed, after preserving project evidence.
- This repository's example resource names and paths are illustrative of the completed lab; avoid publishing account IDs, live endpoints, or secret values in a public repository unless necessary.

## Portfolio highlights

**Skills demonstrated:** AWS Glue, PySpark, SQL, Amazon RDS MySQL, S3 Parquet, Athena, IAM, Secrets Manager, VPC endpoints, JDBC connectivity, ETL troubleshooting, and data validation.

**Resume/LinkedIn description:** Built and validated an AWS ETL pipeline that joins customer and order data from RDS MySQL with AWS Glue/PySpark, stores transformed Parquet data in S3, and analyzes it in Athena; verified 8 orders and $2,159.93 in sales.

## Suggested repository contents

```text
aws-glue-customer-orders-etl/
  README.md
  docs/
    AWS_Glue_Customer_Orders_ETL_Project_Report.pdf
  screenshots/
    athena-validation.png    # optional; redact identifying information first
  sql/
    athena_customer_orders.sql  # optional
  glue/
    customer_orders_etl.py  # optional: complete sanitized runnable script
```

**Evidence:** Athena validation screenshot captured October 20, 2026. The screenshot contains AWS account details; redact before publishing publicly.
