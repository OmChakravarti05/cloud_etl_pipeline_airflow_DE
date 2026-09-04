\# Cloud-Style ETL Pipeline (Airflow + Docker + Postgres)



An end-to-end ETL pipeline built to practice cloud data engineering fundamentals: extracting data from a public source, transforming it, and loading it into a database — orchestrated, monitored, and retry-safe.



\## What it does



1\. \*\*Extract\*\* — Downloads a public CSV dataset via HTTP.

2\. \*\*Transform\*\* — Cleans the data with pandas (removes nulls, normalizes column names).

3\. \*\*Load\*\* — Writes the cleaned data into a PostgreSQL database, using an idempotent load (safe to re-run without duplicating data).



\## Tech stack



\- \*\*Apache Airflow\*\* — orchestration and scheduling (open-source equivalent of Azure Data Factory)

\- \*\*Docker / Docker Compose\*\* — containerized local environment

\- \*\*PostgreSQL\*\* — data storage

\- \*\*Python (pandas, psycopg2, requests)\*\* — extraction and transformation logic



\## Why this stack



This project was built without access to a cloud provider account. Rather than skip hands-on practice, I used open-source tools that mirror the same architectural pattern used in cloud data platforms — orchestration, storage, and transform layers — so the concepts transfer directly to Azure Data Factory, Synapse, or similar tools.



\## Design choices



\- \*\*Retries\*\* — each task retries up to 3 times with a 1-minute delay, handling transient failures gracefully (a common real-world pipeline requirement).

\- \*\*Idempotent load\*\* — the load step truncates and reloads the target table, so re-running the pipeline never creates duplicate data.

\- \*\*Error handling\*\* — the extract step explicitly validates the HTTP response before writing data, rather than failing silently.



\## How to run it



1\. Start the environment: `docker compose up -d`

2\. Open the Airflow UI at `http://localhost:8080`

3\. Trigger the `first\_etl\_pipeline` DAG

4\. Verify the load: `docker exec -it data-postgres psql -U data -d pipeline\_data -c "SELECT COUNT(\*) FROM tips;"`



\## What I'd add next



\- Data quality checks before the load step

\- A scheduled (rather than manual) trigger

\- Unit tests for the transform logic

