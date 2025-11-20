# Airflow to Redshift Serverless – Complete SQL Operations Guide

This README provides a comprehensive reference for executing **all major Redshift Serverless SQL operations** using **Apache Airflow** with the `RedshiftDataOperator`.

Operations covered:

* COPY (S3 → Redshift)
* SELECT (with XCom results)
* INSERT
* UPDATE
* DELETE
* TRUNCATE
* MERGE (Upsert)
* UNLOAD (Redshift → S3)

---

## 1. Requirements

### Install Amazon Provider for Airflow

```bash
pip install apache-airflow-providers-amazon
```

### Airflow Connection

Create a connection in the Airflow UI:

* **Conn ID:** `aws_default`
* **Conn Type:** `Amazon Web Services`
* Configure using Access Keys / IAM Role / etc.

---

## 2. Airflow DAG – All SQL Operations for Redshift Serverless

```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from datetime import datetime

default_args = {"owner": "airflow"}

with DAG(
    dag_id="redshift_serverless_full_sql_operations",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
):

    # 1️⃣ COPY (Load S3 → Redshift)
    copy_cmd = RedshiftDataOperator(
        task_id="copy_data",
        aws_conn_id="aws_default",
        database="analytics",
        workgroup_name="redshift-serverless-wrkgrp",
        sql="""
            COPY public.sales
            FROM 's3://my-bucket/sales/'
            IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftRole'
            FORMAT AS PARQUET;
        """,
        wait_for_completion=True
    )

    # 2️⃣ SELECT (Push result to XCom)
    select_cmd = RedshiftDataOperator(
        task_id="select_data",
        aws_conn_id="aws_default",
        database="analytics",
        workgroup_name="redshift-serverless-wrkgrp",
        sql="SELECT COUNT(*) AS row_count FROM public.sales;",
        wait_for_completion=True,
        return_sql_results=True
    )

    # 3️⃣ INSERT
    insert_cmd = RedshiftDataOperator(
        task_id="insert_data",
        aws_conn_id="aws_default",
        database="analytics",
        workgroup_name="redshift-serverless-wrkgrp",
        sql="""
            INSERT INTO public.sales_summary (sale_date, total_amount)
            SELECT sale_date, SUM(amount)
            FROM public.sales
            GROUP BY sale_date;
        """,
        wait_for_completion=True
    )

    # 4️⃣ UPDATE
    update_cmd = RedshiftDataOperator(
        task_id="update_data",
        aws_conn_id="aws_default",
        database="analytics",
        workgroup_name="redshift-serverless-wrkgrp",
        sql="""
            UPDATE public.sales_summary
            SET total_amount = total_amount * 1.05
            WHERE sale_date = '2024-01-01';
        """,
        wait_for_completion=True
    )

    # 5️⃣ DELETE
    delete_cmd = RedshiftDataOperator(
        task_id="delete_data",
        aws_conn_id="aws_default",
        database="analytics",
        workgroup_name="redshift-serverless-wrkgrp",
        sql="""
            DELETE FROM public.sales
            WHERE amount < 0;
        """,
        wait_for_completion=True
    )

    # 6️⃣ TRUNCATE
    truncate_cmd = RedshiftDataOperator(
        task_id="truncate_data",
        aws_conn_id="aws_default",
        database="analytics",
        workgroup_name="redshift-serverless-wrkgrp",
        sql="TRUNCATE TABLE public.temp_stage;",
        wait_for_completion=True
    )

    # 7️⃣ MERGE (UPSERT)
    merge_cmd = RedshiftDataOperator(
        task_id="merge_data",
        aws_conn_id="aws_default",
        database="analytics",
        workgroup_name="redshift-serverless-wrkgrp",
        sql="""
            MERGE INTO public.customers AS tgt
            USING public.staging_customers AS src
            ON tgt.customer_id = src.customer_id
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.name = src.name,
                    tgt.phone = src.phone
            WHEN NOT MATCHED THEN
                INSERT (customer_id, name, phone)
                VALUES (src.customer_id, src.name, src.phone);
        """,
        wait_for_completion=True
    )

    # 8️⃣ UNLOAD (Redshift → S3)
    unload_cmd = RedshiftDataOperator(
        task_id="unload_to_s3",
        aws_conn_id="aws_default",
        database="analytics",
        workgroup_name="redshift-serverless-wrkgrp",
        sql="""
            UNLOAD ('SELECT * FROM public.sales_summary')
            TO 's3://my-bucket/unload/sales_summary_'
            IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftRole'
            PARQUET;
        """,
        wait_for_completion=True
    )

    # Task chaining
    copy_cmd >> select_cmd >> insert_cmd >> update_cmd >> delete_cmd >> truncate_cmd >> merge_cmd >> unload_cmd
```

---

## 3. UNLOAD Syntax Cheat Sheet

### Basic

```sql
UNLOAD ('SELECT * FROM table')
TO 's3://bucket/prefix_'
IAM_ROLE 'arn:aws:iam::123:role/RedshiftRole';
```

### PARQUET

```sql
UNLOAD ('SELECT * FROM table')
TO 's3://bucket/path_'
IAM_ROLE 'arn:aws:iam::123:role/RedshiftRole'
PARQUET;
```

### CSV

```sql
UNLOAD ('SELECT * FROM table')
TO 's3://bucket/path_'
IAM_ROLE 'arn:aws:iam::123:role/RedshiftRole'
CSV
ALLOWOVERWRITE
HEADER;
```

---

## 4. Summary

This guide provides a ready-to-use Airflow implementation for all common Redshift Serverless SQL operations. Use it as a reference or plug it directly into your project.

If you want a **modular version**, **Jinja-templated SQL**, or **production folder structure**, let me know!
