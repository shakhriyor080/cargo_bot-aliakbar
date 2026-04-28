# Cargo Bot

Telegram-based cargo monitoring bot for Uzbekistan logistics groups. Monitors public Telegram cargo groups in real time and notifies users about cargo matching their route and filters.

Built with **aiogram 3** (bot interface) + **Telethon** (group monitoring via user account) + **SQLite** (persistence).

## Features

### Core
- **Multi-language UI** — Uzbek / Russian / English, per-user
- **Real-time monitoring** of cargo groups (live push notifications)
- **Today's cargo search** with results cached for 5 minutes
- **Direction-aware matching** — `Bukhara → Tashkent` does not match `Tashkent → Bukhara`
- **Declension-tolerant city stems** — handles Russian/Uzbek forms (`Бухара`, `Бухары`, `Бухаре`, …)
- **Authentication** with PBKDF2-SHA256 hashed passwords
- **Per-user dedup** — no cargo is sent to the same user twice (24h TTL)

### Cargo filters
Each user can configure filters that apply to **both** historical search and live notifications:

| Filter | Example | Effect |
|---|---|---|
| `weight` | `5-25` / `min 10` / `max 25` | Only cargo whose declared weight (in tons) overlaps the range |
| `truck` | `тент`, `реф`, `isuzu` | Cargo description must contain this substring |
| `phone` | `on` / `off` | Only cargo with a detected phone number |
| `include` | `мука,рис` | All comma-separated keywords must be present |
| `exclude` | `отказ,штраф` | None of these keywords may be present |

### Admin
- Add/remove monitored groups **from the bot itself** (no code edits)
- View user statistics (total / authorized / with routes / active 24h)
- Send broadcast announcements to all authorized users

## Installation

### Requirements
- Python 3.9+ (uses `zoneinfo`)
- Telegram bot token ([@BotFather](https://t.me/BotFather))
- Telegram API ID & hash ([my.telegram.org](https://my.telegram.org/apps))
- A Telegram **user account** that is a member of the cargo groups you want to monitor

### Setup

```bash
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your tokens:

```env
BOT_TOKEN=123456:abcdef...
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=your-api-hash
```

### First run

```bash
python monitor_groups.py
```

On first start:
1. Telethon will ask for your phone number and verification code (interactive). After that, `session.session` is created — the bot reuses it on subsequent runs.
2. SQLite `bot.db` is auto-created with default schema and seed data:
   - Two logins: `admin` / `123456` and `logistics` / `password` (change in `.env` before first run)
   - 8 default cargo groups (defined in `DEFAULT_GROUPS` in `monitor_groups.py`)

## User flow

```
/start
  ↓
🌐 Choose language (uz / ru / en)
  ↓
👋 Enter your login → enter password
  ↓
✅ Authorized
  ↓
Send a route, e.g.:  Bukhara Tashkent
  ↓
🔎 Bot searches today's messages from all monitored groups
  ↓
🚛 Cargo matching the route + your filters is sent
  ↓
🔔 New live cargo on your route is pushed automatically
```

## Commands

### User commands
| Command | Description |
|---|---|
| `/start` | Log in (asks language, then login/password) |
| `/logout` | Log out |
| `/lang` | Change language without re-login |
| `/help` | Full command reference |
| `/myroute` | Show current route + notification status |
| `/clearroute` | Stop monitoring this route |
| `/notify on\|off` | Toggle live notifications |
| `/filters` | Show current filters |
| `/setfilter <type> <value>` | Set or clear a filter |
| `/clearfilters` | Clear all filters |
| `/stats` | Personal statistics |

To **search**: just send a route. Three formats are accepted:
- `Bukhara Tashkent`
- `Bukhara - Tashkent`
- `Bukhara ➜ Tashkent`

### Filter commands

```
/setfilter weight 5-25       # weight range, in tons
/setfilter weight min 10     # only minimum
/setfilter weight max 25     # only maximum
/setfilter weight clear      # remove weight filter

/setfilter truck тент        # truck type substring
/setfilter truck clear

/setfilter phone on          # require phone number
/setfilter phone off

/setfilter include мука,рис  # comma-separated, all must match
/setfilter include clear

/setfilter exclude отказ     # comma-separated, none may match
/setfilter exclude clear

/clearfilters                # remove all filters at once
```

### Admin commands
| Command | Description |
|---|---|
| `/groups` | List monitored groups |
| `/addgroup <username>` | Add a group (accepts `@name`, `name`, or `https://t.me/name`) |
| `/delgroup <username>` | Remove a group |
| `/users` | Total / authorized / with routes / active 24h, plus group count |
| `/broadcast <text>` | Send announcement to all authorized users (in their language) |

## How it works

### Group monitoring
The bot uses **two clients** simultaneously:
- `aiogram.Bot` — handles user-facing commands and sends notifications
- `telethon.TelegramClient` — logs in as your user account, joins the cargo groups, and reads messages in real time

`@client.on(events.NewMessage())` fires for every incoming message in any chat your account is in. The handler filters by group username (cached in memory, 30s TTL) and applies route + filter matching for each authorized user.

### Search caching
When a user runs a search, the bot reads up to 2000 latest messages per group (newest-first, stops at yesterday). Results are cached by `(from_city, to_city)` for 5 minutes — repeated searches by other users don't re-hit Telegram. Filters are applied **per-user after** the cache lookup, so users with different filters share the cache.

### Cargo deduplication
Each cargo block hashes to a SHA-256 of its normalized text. The `(chat_id, cargo_hash)` pair is stored in `sent_cargos`, so each user sees a given cargo at most once. Rows older than 24h are cleaned hourly.

### Direction-aware matching
A cargo block matches a route only if:
1. Both `from_city` and `to_city` (as **stems**, e.g. `буха`, `ташке`) appear in the normalized text
2. `from_city`'s position is **before** `to_city`'s position in the text

This way `Ташкент → Бухара` will not match a `Бухара → Ташкент` request.

## Database schema

```sql
credentials (login, password_hash, is_admin)
users       (chat_id, login, is_authorized, language, from_city, to_city,
             total_searches, total_cargos_received, updated_at)
groups      (username, title, added_by, added_at)
user_filters(chat_id, weight_min, weight_max, truck_type, require_phone,
             keywords_include, keywords_exclude, notifications_enabled, updated_at)
sent_cargos (chat_id, cargo_hash, sent_at)  -- (chat_id, cargo_hash) PK; sent_at indexed
```

Schema migrations are applied automatically on startup (`ALTER TABLE` for older DBs).

## Configuration (`.env`)

| Variable | Default | Description |
|---|---|---|
| `BOT_TOKEN` | — | Telegram bot token |
| `TELEGRAM_API_ID` / `_HASH` | — | Telethon API credentials |
| `SESSION_NAME` | `session` | Telethon session filename (without `.session`) |
| `DATABASE_PATH` | `bot.db` | SQLite database path |
| `TIMEZONE` | `Asia/Tashkent` | Timezone for "today" calculations |
| `DEFAULT_LANGUAGE` | `ru` | Fallback language |
| `SENT_CARGO_TTL_HOURS` | `24` | How long to remember a sent cargo |
| `SEARCH_CACHE_TTL_SECONDS` | `300` | Search result cache TTL |
| `SEARCH_MESSAGE_LIMIT` | `2000` | Max messages read per group per search |
| `BROADCAST_RATE_DELAY` | `0.05` | Sleep between broadcast messages (seconds) |
| `INITIAL_ADMIN_PASSWORD` | `123456` | Seeded only on **first** run |
| `INITIAL_LOGISTICS_PASSWORD` | `password` | Seeded only on **first** run |

## Project structure

```
cargo_bot/
├── monitor_groups.py    # bot entrypoint (everything)
├── .env                 # secrets (gitignored)
├── .env.example         # template
├── .gitignore
├── requirements.txt
├── README.md
├── session.session      # Telethon auth (gitignored, created on first run)
├── bot.db               # SQLite (gitignored, created on first run)
└── logs/                # rotated bot.log (gitignored)
```

## Security notes

- `.env`, `*.session`, `*.db` are gitignored — never commit them
- Passwords are hashed with PBKDF2-SHA256 (200 000 iterations, 16-byte salt)
- The bot uses a **personal user account** for Telethon — treat `session.session` as a credential
- The default seeded passwords (`123456` / `password`) **must** be changed before deploying — set `INITIAL_*_PASSWORD` in `.env` and delete `bot.db` so they're re-seeded with your values

## Troubleshooting

- **Bot doesn't react to commands**: check that `python monitor_groups.py` is running. Check `logs/bot.log`.
- **No cargo found, but I know there's some**: your user account must be a member of the configured groups. Try `/groups` (admin) and verify; add new ones with `/addgroup`.
- **`get_entity failed`** when adding a group: the user account is not a member of that group, or the username is private/wrong.
- **Live notifications don't arrive**: check `/notify on`, check `/myroute` is set, check filters with `/filters` aren't too restrictive.
- **Bot sees no new messages**: Telethon session may be expired — delete `session.session` and re-run to re-authenticate.
- **All passwords reset**: delete `bot.db` (this also clears users, routes, filters, sent-cargo history).

## Development

The whole bot is one file (`monitor_groups.py`) for simplicity, organized into clearly separated sections:

```
CONFIG / LOGGING / DEFAULT_GROUPS
TRANSLATIONS (uz / ru / en)
PASSWORD HASHING
DATABASE  (init, credentials, language, routes, sent, groups, filters, stats)
GROUP CACHE (in-memory, 30s TTL)
TEXT HELPERS (normalize, route parsing, city stems, weight extraction, phones)
FILTER LOGIC
TTL CACHE (search results, 5min)
HANDLERS  (auth → route → filters → admin → catch-all search)
LIVE TELETHON HANDLER
BACKGROUND CLEANUP
MAIN
```

To extend:
- New filter type → add column to `user_filters`, new branch in `cmd_setfilter`, check in `cargo_passes_filters`
- New language → add a key in `TRANSLATIONS`, update `LANG_BUTTONS` and `parse_language_choice`
- New admin command → add a `Command(...)` handler with `db_is_admin(chat_id)` guard before catch-all
