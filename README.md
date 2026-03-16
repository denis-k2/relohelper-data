# Data Scrapers for Relohelper

Этот репозиторий содержит скрипты сбора и подготовки данных, используемых в проекте Relohelper.  
Сценарии на Python выполняют получение данных из внешних источников, их очистку и загрузку в базу данных PostgreSQL.

Подготовленная база данных используется серверным приложением, реализованным на Go, которое предоставляет доступ к данным через HTTP API.

API-сервер проекта находится в отдельном репозитории:  
[https://github.com/denis-k2/relohelper-go](https://github.com/denis-k2/relohelper-go)

---

# Scrapers: Numbeo + Climate

В проекте три основных исполняемых скрипта:

1. `8_create_numbeo_tables.py` -> создает/обновляет справочники Numbeo (`numbeo_cost_categories`, `numbeo_cost_params`)
2. `9_scraping_numbeo_stats.py` -> цены Numbeo -> `numbeo_city_costs`
3. `10_scraping_climate.py` -> климат Weather Atlas -> `avg_climate`

---

## Подготовительные ноутбуки

Ноутбуки использовались для подготовки исходных данных и загрузки справочников в БД:

1. `1_country_codes.ipynb`  
   Парсинг стран и их кодов с загрузкой в БД.

2. `2_numbeo_cities.ipynb`  
   Подготовка основного DataFrame по городам Numbeo и сохранение промежуточных `.pkl`.

3. `3_merge_2023_data.ipynb`  
   Объединение новых данных со старыми данными 2023 года для перехода на `geonameid` как основной идентификатор города.

4. `4.1_cities_to_bd.ipynb`  
   Загрузка списка городов с базовой географической информацией и индексами Numbeo в БД.

5. `4_add_geonames.ipynb`  
   Добавление `geonameid`, населения и координат к подготовленному набору городов.

6. `5_numbeo_countries.ipynb`  
   Формирование DataFrame по страновым индексам Numbeo.

7. `6_legatum.ipynb`  
   Подготовка и загрузка в БД данных Legatum Prosperity Index 2023.

8. `7_preparations scraping climate.ipynb`  
   Подготовка данных для климатического скрапера: выделение городов США в отдельный набор из-за проблем с единицами измерения на Weather Atlas.

---

## Справочники Numbeo

Подготовка справочников категорий и параметров Numbeo:

```bash
python 8_create_numbeo_tables.py
```

Скрипт создает или обновляет:

```text
numbeo_cost_categories
numbeo_cost_params
```

---

## Numbeo (`9_scraping_numbeo_stats.py`)

Источник городов:

```text
data/geonameid.pkl
```

Целевая таблица:

```text
numbeo_city_costs (PRIMARY KEY: geoname_id, param_id)
```

Логика работы:

1. Читает города из `geonameid.pkl`.
2. Перед скрапингом проверяет, есть ли уже данные по `geoname_id` в `numbeo_city_costs`.
3. Если данные есть, город пропускается (`Skipped`).
4. Если данных нет, город скрапится и пишется в БД через upsert.
5. `--limit N` применяется к первым **N отсутствующим** в БД городам (а не к первым N строкам файла).

HTTP-поведение:

1. `429` (rate limit) -> fail-fast (без долгого ожидания на этом городе).
2. Ретраи выполняются только для `500/502/503/504`.
3. Между запросами используется случайная пауза:
   `REQUEST_DELAY_MIN_SECONDS = 5`, `REQUEST_DELAY_MAX_SECONDS = 12`
   (равномерно случайный интервал 5-12 сек), чтобы снизить риск
   срабатывания rate limit.
4. Для части городов возможны устойчивые `ReadTimeout`; такие города можно временно исключать из онлайн-прогона и обрабатывать отдельно.

Запуск:

```bash
python 9_scraping_numbeo_stats.py --limit 15
python 9_scraping_numbeo_stats.py
```

Логи:

```text
data/logs_numbeo_city_costs.log
```

###### Manual/offline режим (без запросов в интернет):

1. Сохранить HTML-страницы в `data/manual_numbeo_html/` с именем файла = `geonameid` (например, `2643743.html`).
2. Запустить:

```bash
python 9_scraping_numbeo_stats_manual.py
```

Лог manual-режима:

```text
data/logs_numbeo_city_costs_manual.log
```

---

## Climate (`10_scraping_climate.py`)

Источник городов:

```text
./2026_2/data/geonameid.pkl
```

Целевая таблица:

```text
avg_climate (PRIMARY KEY: city_id, month)
```

Результаты запуска:

```text
./2026_2/data/climate_links.pkl
./2026_2/data/missing_climate_links.pkl
```

Ручные корректировки ссылок:

```text
./2026_2/data/correct_urls.json
```

Запуск:

```bash
python 10_scraping_climate.py
python 10_scraping_climate.py --retry-missing
```

---

## Установка через uv

В проекте используются:

```text
pyproject.toml
uv.lock
```

Установка зависимостей:

```bash
uv sync
```
