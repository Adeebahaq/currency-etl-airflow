# Currency Exchange ETL (Airflow + MongoDB)

A daily Airflow pipeline, built with the Astro CLI, that loads EUR exchange rates from the Frankfurter API into MongoDB.

## Tasks
1. extract_rates: calls https://api.frankfurter.dev/v2/rates and pushes the raw rates to XCom (key: raw_rates)
2. transform_rates: pulls raw_rates, builds the required documents and pushes them (key: transformed_rates)
3. load_to_mongodb: pulls transformed_rates and upserts them into MongoDB

## Services (docker-compose.override.yml)
- mongo: MongoDB 7, network "airflow", persistent volume mongo_data
- mongo-express: web admin UI on http://localhost:8081

## Run
1. Install Docker Desktop and the Astro CLI
2. Copy .env.example to .env
3. astro dev start
4. Airflow: http://localhost:8080. Trigger DAG currency_exchange_etl
5. Mongo Express: http://localhost:8081 (database currency_db, collection exchange_rates)