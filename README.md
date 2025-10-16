# HSE: Системы хранения данных — Итоговое задание

## Data Vault для SampleSuperstore

Проект реализует полноценное хранилище данных на основе методологии Data Vault 2.0 для анализа продаж из датасета SampleSuperstore. Реализация выполнена в Yandex Cloud Managed Greenplum с автоматизированным ETL процессом.


## Настройка окружения

Подключение к Greenplum/PostgreSQL настраивается через переменные окружения (только новые универсальные имена DB_*).

Переменные:
- DB_HOSTS — список хостов через запятую (или используйте DB_HOST для одного хоста)
- DB_PORT — порт, по умолчанию 6432
- DB_NAME — имя базы, по умолчанию postgres
- DB_USER — пользователь (обязательно)
- DB_PASSWORD — пароль (обязательно)
- DB_TARGET_SESSION_ATTRS — по умолчанию read-write
- DB_SSLMODE — по умолчанию verify-full
- DB_SSLROOTCERT — путь к корневому сертификату, по умолчанию ~/.postgresql/root.crt

Параметры ETL:
- ETL_TRUNCATE_STAGE — если true (по умолчанию), стадийная таблица очищается перед загрузкой
  - для обратной совместимости поддерживается TRUNCATE_STAGE

## Запуск SQL скриптов

Рекомендуемый способ: новый модуль-обертка.

- python -m etl.sql_runner 00_init_schemas.sql 01_staging_sample_superstore.sql
- python -m etl.sql_runner 10_dv_hubs.sql 15_alias_views.sql 20_dv_links_and_satellites.sql 40_dv_constraints_and_indexes.sql 50_etl_load_from_stage.sql 60_verify_metrics.sql

Если аргументы не переданы, выполняется дефолтный порядок с новыми именами.

По-прежнему можно использовать старый модуль:
- python -m etl.run_sql ...

## Конвейер «все-в-одном»

- python -m etl.pipeline --csv SampleSuperstore.csv

Старый модуль
- python -m etl.run_all --csv SampleSuperstore.csv
