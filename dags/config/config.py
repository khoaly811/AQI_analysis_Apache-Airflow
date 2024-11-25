# config/config.py

# Directory containing source CSV files
SOURCE_DIR = '/opt/airflow/dags'

# Database connection details for PostgreSQL
# DB_CONFIG = {
#     'database': 'bi_seminar',
#     'username': 'airflow',
#     'password': '4NDBdytYKvVa3zrrNvv32JDoHUwwXGTJ',
#     'host': 'dpg-csnim40gph6c73bf8l00-a.singapore-postgres.render.com',
#     'port': 5432
# }
DB_CONFIG = {
    'database': 'bi_seminar',
    'username': 'airflow',
    'password': 'airflow',
    'host': 'host.docker.internal',
    'port': 5432
}
