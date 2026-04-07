# bb

OTP-приложение на Erlang: периодически тянет список продуктов с [dummyjson.com](https://dummyjson.com), складывает ответы в память (ETS + индексы в `gen_server`) и отдаёт HTTP API.

## Что сделано

- **`bb_dummyjson`** — по HTTPS (gun) получает продукты, парсит JSON и дописывает ответ в хранилище; интервал опроса настраивается (по умолчанию из `config/sys.config`: `fetch_interval_ms`).
- **`bb_products_store`** — хранит ответы `{timestamp_mcs, [{product_id, price}, ...]}`; выборка с фильтрами по датам и по id продукта.
- **HTTP**: маршруты в `bb_http.erl`.

## Сборка и запуск

```bash
rebar3 compile
rebar3 shell
```

Конфигурация: `config/sys.config` — `http_port` (порт сервера; если не задан — 8080 в `bb_http`), `fetch_interval_ms` (стартовый интервал опроса; если не задан или невалиден — 30 000 мс в коде `bb_dummyjson`).

## API

| Метод | Путь | Описание |
|-------|------|-----------|
| `GET` | `/assembled-products` | Query: `start_date`, `end_date` — `YYYY-MM-DD` (опционально), `id` — id продукта (опционально). Ответ — **один JSON-объект**: ключи — метки времени в UTC ISO 8601 с миллисекундами (например `2025-10-10T18:10:50.622Z`), значения — массивы вида `{"id": "<строка>", "price": число}`. Снимки отсортированы по времени. При пустой выборке — `[]`. |
| `POST` | `/set-time` | Тело JSON: `interval_ms` — положительное число миллисекунд; задаёт новый интервал опроса dummyjson и перезапускает таймер. |

Ошибки валидации — `400` с телом `{"error":"..."}`.

## TODO

- Пагинация для `/assembled-products` (`limit`/`offset`) — при длительной работе с частым интервалом опроса ответ может стать очень большим.

## Тесты

```bash
rebar3 eunit    # юнит-тесты bb_products_store
rebar3 ct       # интеграционные тесты (требуют доступ к dummyjson.com)
```

Интеграционные тесты (`bb_integration_SUITE`) поднимают полное приложение и обращаются к реальному API dummyjson.com — для запуска необходима сеть.

## CI

`.github/workflows/erlang-checks.yaml`: сборка, `fmt -c`, lint (elvis), xref, dialyzer, eunit/ct (reusable workflow valitydev/erlang-workflows).
