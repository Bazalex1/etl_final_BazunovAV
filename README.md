# Итоговое ДЗ по модулю 3 — ETL-процессы

В этом проекте реализован ETL-процесс с использованием MongoDB, PostgreSQL и Apache Airflow [file:1].

Что сделано ( подтверждающие скриншоты находятся в папке screenshots):
- подняты MongoDB и PostgreSQL (screen2.png)
- сгенерированы тетовые данные в MongoDB (screen1.png)
- настроен ETL-пайплайн для загрузки данных из MongodB в PostgreSQL через airflow ((screen3.png), (screen4.png), (screen5.png))
- выполнены трансформации данных при загрузке: рассчитаны новые поля, а загрузка настроена без дублей (screen10.png) (На скриншоте можно увидеть новые вычисляемые поля, а также отсусвие дублей)
- построены 2 аналитические витрины  в Airflow (с screen6 по screen9)

## Структура проекта

etl_final_project/
├── dags/
│   ├── etl_mongo_to_postgres.py
│   └── build_datamarts.py
├── scripts/
│   └── generator.py
├── sql/
│   └── create_tables.sql
├── logs/
├── screenshots/
├── docker-compose.yml
└── README.md
## Какие данные используются:


В MongoDB созданы 3 коллекции (взяты из примеров к заданию):

user_sessions — сессии пользователей 

event_logs — логи событий 

support_tickets — обращения в поддержку 




После генрации данных было создано:

- 1000 записей в user_sessions

- 5509 записей в event_logs

- 300 записей в support_tickets

## Что хранится в PostgreSqL
В PostgreSQL созданы  staging-таблицы:

- stg_user_sessions

- stg_event_logs

- stg_support_tickets

и 2 аналитические витрины:

- mart_user_activity — активность пользователей 

- mart_support_performance — эффективность поддержки 

## Какие трансформации сделаны
Во время загрузки данных из MongoDB в PostgreSQL выполняются  преобразования:

- рассчитывается длительность сессии в минутах

- считается количество посещённых страниц

- считается количество действий в сессии

- рассчитывается время решения тикета

- считается количество сообщений в тикете

- Для защиты от дублей используется загрузка через ON CONFLICT.

# Как запустить проект
Запустить контейнеры:

bash
docker compose up -d

Открыть Airflow:
http://localhost:8080
Логин и пароль:
admin / admin

Сгенерировать данные в MongoDB:

bash
docker exec -it airflow_webserver bash
python /opt/airflow/scripts/generator.py

Создать таблцы в PostgreSQL:
bash
docker exec -i etl_postgres psql -U airflow -d airflow < sql/create_tables.sql


Запустить DAG etl_mongo_to_postgres


Запустить DAG build_datamarts

## Что делают DAG

### etl_mongo_to_postgres
Этот DAG:

читает данные из MongoDB

преобразует их

загружает в PostgreSQL


### build_datamarts
Этот DAG:

строит витрину по активности пользователей

строит витрину по обращениям в поддержку

## Примеры проверок
Проверка количества строк в staging-таблицах:

sql
SELECT COUNT(*) FROM stg_user_sessions;
SELECT COUNT(*) FROM stg_event_logs;
SELECT COUNT(*) FROM stg_support_tickets;

Проверка витрин:

sql
SELECT COUNT(*) FROM mart_user_activity;
SELECT COUNT(*) FROM mart_support_performance;


Просмотр данных:

sql
SELECT * FROM mart_user_activity LIMIT 10;
SELECT * FROM mart_support_performance LIMIT 10;

## Итог
В проекте реализована рабочая ETL-система с MongoDB, PostgreSQL и Airflow, включая генерацию данных, репликацию, тронсфармации и построение двух аналитических витрин.