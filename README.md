
# Weather Atlas Climate Scraper

Скрипт собирает климатические данные городов с **weather-atlas.com** и сохраняет их в таблицу PostgreSQL `avg_climate`.

Источник городов:

```
data/geonameid.pkl
```

Для каждого города формируется URL страницы климата, данные парсятся по месяцам и записываются в БД.

Ключ таблицы:

```
PRIMARY KEY (city_id, month)
```

Повторные запуски обновляют существующие записи.

---

# Результаты работы

После запуска создаются:

```
data/climate_links.pkl
```

успешно найденные ссылки Weather Atlas

```
data/missing_climate_links.pkl
```

города, для которых ссылка не была определена автоматически

---

# Ручные исправления

Для некоторых городов требуется указать правильную ссылку в:

```
correct_urls.json
```

пример:

```json
{
  "162": "https://www.weather-atlas.com/en/belgium/ghent-climate"
}
```

---

# Запуск

Полный запуск:

```
python scraping_climate.py
```

Повторный запуск только для проблемных городов:

```
python scraping_climate.py --retry-missing
```

В этом режиме используются города из `missing_climate_links.pkl`.

---

# Требования

```
pandas
requests
beautifulsoup4
psycopg2
lxml
```
