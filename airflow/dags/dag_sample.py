from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import csv

# Аргументы по умолчанию: владелец процесса и время отсчёта для задачи
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
}

# Функция для чтения данных и генерации SQL-запросов
def generate_insert_queries():
    CSV_FILE_PATH = 'sample_files/sample.csv'
    with open( CSV_FILE_PATH, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
    
        # Генерим запросы
        insert_queries = []
        is_header = True
        for row in csvreader:
            if is_header:
                is_header = False
                continue
            insert_query = f"INSERT INTO user_reports (id,order_number,total,discount,buyer_id) VALUES ({row[0]}, {row[1]}, {row[2]},{row[3]},{row[4]});"
            insert_queries.append(insert_query)
        
        # Сохраняем запросы
        with open('./dags/sql/insert_queries.sql', 'w') as f:
            for query in insert_queries:
                f.write(f"{query}\n")

# Определяем DAG
with DAG('csv_to_postgres_dag',
         default_args=default_args, #аргументы по умолчанию в начале скрипта
         schedule_interval='@once', #запускаем один раз
         catchup=False) as dag: #предотвращает повторное выполнение DAG для пропущенных расписаний.

    # Создаём таблицу в PostgreSQL
    create_table = PostgresOperator(
        task_id='create_table', #идентификатор задачи
        postgres_conn_id='write_to_postgres',  # Название подключения
        sql="""
        CREATE TABLE IF NOT EXISTS crmClients (
            user_id          TEXT PRIMARY KEY,
            full_name        TEXT NOT NULL,
            status          TEXT NOT NULL
        );

        INSERT INTO crmClients (user_id, full_name, status) VALUES
            ('user1',     'Prothetic One',   'active'),
            ('user2',     'Prothetic Two',   'active'),
            ('user3',     'Prothetic Three', 'active')
        ON CONFLICT (user_id) DO NOTHING;

        CREATE TABLE IF NOT EXISTS telemetryEvents (
            id          BIGSERIAL PRIMARY KEY,
            user_id     TEXT NOT NULL,
            event_time  TIMESTAMPTZ NOT NULL,
            event_type  INT NOT NULL DEFAULT 0
        );

        INSERT INTO telemetryEvents (user_id, event_time, event_type)
        SELECT
            u.user_id,
            ((CURRENT_DATE - d.offset_days)::timestamp + (h.hour_of_day || ' hour')::interval)
                AT TIME ZONE 'UTC' AS event_time,
            floor(random() * 100)::int AS event_type
        FROM (VALUES ('user1'), ('user2'), ('user3')) AS u(user_id)
        CROSS JOIN generate_series(1, 30) AS d(offset_days)
        CROSS JOIN generate_series(0, 23, 4) AS h(hour_of_day)
        WHERE NOT EXISTS (SELECT 1 FROM telemetryEvents LIMIT 1);

        CREATE TABLE IF NOT EXISTS client_event_daily_aggregates (
            user_id     TEXT NOT NULL,
            event_date  DATE NOT NULL,
            full_name   TEXT NOT NULL,
            status      TEXT NOT NULL,
            event_type  INT,
            event_count BIGINT NOT NULL,
            PRIMARY KEY (user_id, event_date, event_type)
        );
        """
    )

    #Запускаем выполнение оператора PostgresOperator
    run_insert_queries = PostgresOperator(
        task_id='run_insert_queries',
        postgres_conn_id='write_to_postgres',  # Название подключения к PostgreSQL в Airflow UI
        sql="""
          INSERT INTO client_event_daily_aggregates (
                user_id, event_date, full_name, status, event_type, event_count
            )
            SELECT
                c.user_id,
                (e.event_time AT TIME ZONE 'UTC')::date AS event_date,
                c.full_name,
                c.status,
                e.event_type,
                COUNT(e.id) AS event_count
            FROM crmClients AS c
            JOIN telemetryEvents AS e
                ON e.user_id = c.user_id
            GROUP BY
                c.user_id,
                (e.event_time AT TIME ZONE 'UTC')::date,
                c.full_name,
                c.status,
                e.event_type
            ON CONFLICT (user_id, event_date, event_type) DO UPDATE
            SET full_name   = EXCLUDED.full_name,
                status      = EXCLUDED.status,
                event_count = EXCLUDED.event_count;
        """
    )
    create_table>>run_insert_queries
    # Тут дальше можно продолжать пайплайн