# Scrapers: Numbeo + Climate

В проекте два независимых скрапера:

1. `9_scraping_numbeo_stats.py` -> цены Numbeo -> `numbeo_city_costs`
2. `10_scraping_climate.py` -> климат Weather Atlas -> `avg_climate`

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
   `REQUEST_DELAY_MIN_SECONDS = 30`, `REQUEST_DELAY_MAX_SECONDS = 180`
   (равномерно случайный интервал 30-180 сек), чтобы снизить риск
   срабатывания rate limit.

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

## Зависимости

```text
pandas
requests
beautifulsoup4
psycopg2
lxml
python-dotenv
```
