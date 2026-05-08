# Arbitrage Tracker — UTEX vs Hyperliquid

Трекер разницы цен между биржами UTEX и Hyperliquid в реальном времени.

## Что умеет

- График цен двух бирж в реальном времени (тиковые данные)
- Выбор таймфрейма: 20s / 1m / 15m / 30m / 1h / 1d
- Добавление своих пар (любой символ UTEX + любой символ Hyperliquid)
- Отображение спреда в %
- Звуковой алерт при большом спреде
- Тёмная тема

## Запуск локально

```bash
npm install
npm start
```

Открой http://localhost:3000

## Деплой на Railway (бесплатно)

1. Зайди на https://railway.app и зарегистрируйся (через GitHub)
2. Нажми **New Project → Deploy from GitHub repo**
3. Загрузи эту папку как GitHub репозиторий (или используй Railway CLI)
4. Railway автоматически найдёт `package.json` и запустит сервер
5. Получишь публичный URL вида `https://your-app.railway.app`

### Через Railway CLI (быстрее):

```bash
npm install -g @railway/cli
railway login
railway init
railway up
railway domain
```

## Структура

```
arbitrage/
├── server.js          # Node.js сервер + WebSocket прокси
├── package.json
├── railway.json       # Конфиг для Railway
├── pairs.json         # Сохранённые пары (создаётся автоматически)
└── public/
    └── index.html     # Фронтенд
```

## Добавление пары

1. Нажми **+ Добавить** в левой панели
2. Введи название (например: "BTC Arb")
3. Символ на UTEX: например `BTC`
4. Символ на Hyperliquid: например `BTC`
5. Нажми **Добавить**

Данные появятся на графике как только биржи пришлют первые тики.

## Примечание по UTEX

UTEX не имеет официального публичного API. Соединение идёт через их внутренний WebSocket
`wss://ususdt-api-margin.utex.io/ws`. Если они изменят формат сообщений — потребуется обновить
функцию `handleUtexData` в `server.js`.
