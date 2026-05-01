"""Cargo monitor bot - multi-language Telegram cargo group monitor with filters."""
import asyncio
import hashlib
import logging
import os
import re
import secrets
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove,
)
from dotenv import load_dotenv

# ============================================================
# CONFIG
# ============================================================
load_dotenv()

BOT_TOKEN = os.environ["BOT_TOKEN"]
# API_ID / API_HASH are no longer required (bot-only mode); kept optional
# so existing .env files don't break.
API_ID = int(os.getenv("TELEGRAM_API_ID", "0") or "0")
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
SESSION_NAME = os.getenv("SESSION_NAME", "session")
DATABASE_PATH = os.getenv("DATABASE_PATH", "bot.db")
TIMEZONE_NAME = os.getenv("TIMEZONE", "Asia/Tashkent")
SENT_CARGO_TTL_HOURS = int(os.getenv("SENT_CARGO_TTL_HOURS", "24"))
SEARCH_CACHE_TTL_SECONDS = int(os.getenv("SEARCH_CACHE_TTL_SECONDS", "60"))
SEARCH_MESSAGE_LIMIT = int(os.getenv("SEARCH_MESSAGE_LIMIT", "5000"))
SEARCH_LOOKBACK_HOURS = int(os.getenv("SEARCH_LOOKBACK_HOURS", "18"))
INITIAL_ADMIN_PASSWORD = os.getenv("INITIAL_ADMIN_PASSWORD", "123456")
INITIAL_LOGISTICS_PASSWORD = os.getenv("INITIAL_LOGISTICS_PASSWORD", "password")
DEFAULT_LANGUAGE = os.getenv("DEFAULT_LANGUAGE", "ru")
BROADCAST_RATE_DELAY = float(os.getenv("BROADCAST_RATE_DELAY", "0.05"))

UZ_TIME = ZoneInfo(TIMEZONE_NAME)

# ============================================================
# LOGGING
# ============================================================
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "bot.log", encoding="utf-8"),
    ],
)
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("aiogram").setLevel(logging.INFO)
log = logging.getLogger("cargo_bot")

# ============================================================
# DEFAULT GROUPS (seed on first run)
# ============================================================
DEFAULT_GROUPS = [
    ("vertex_global_logistics", "✨Vertex Gl🌏bal Logistics✨🚛📡"),
    ("yuk95", "🇺🇿 ЮК 🚚 ГРУППА 🚛"),
    ("yukmarkazi_furalar", "YUK🎯markazi |🇺🇿 mahalliy"),
    ("lognumber1", "Логистика - Logistics"),
    ("yuklogistik4432", "🌎DUNYO BOYLAB YUK.1.🗺️"),
    ("olottransport", "🇹🇲Олот Транспорт🇺🇿"),
    ("uzbekistonboylabyukla", "Uzbekiston boylab 🇺🇿Ўзбекистон бўйлаб 🇺🇿"),
    ("logistikasn", "Логистика Снг Европа Азия"),
]

# ============================================================
# TRANSLATIONS
# ============================================================
LANGUAGES = ("uz", "ru", "en")
LANG_BUTTONS = {
    "uz": "🇺🇿 O'zbekcha",
    "ru": "🇷🇺 Русский",
    "en": "🇬🇧 English",
}

TRANSLATIONS = {
    "uz": {
        # Language
        "choose_language": "🌐 Tilni tanlang / Выберите язык / Choose language:",
        "language_set": "✅ Til o'rnatildi: O'zbekcha",
        "lang_prompt": "🌐 Yangi tilni tanlang:",
        # Auth
        "welcome": "👋 Salom! Botdan foydalanish uchun avtorizatsiya kerak.",
        "enter_login": "Loginni kiriting:",
        "enter_password": "Parolni kiriting:",
        "login_empty": "Login bo'sh bo'lishi mumkin emas. Loginni kiriting:",
        "auth_success": "✅ Avtorizatsiya muvaffaqiyatli!\nEndi yuk qidirishingiz mumkin.\nMarshrut kiriting, masalan:\nBuxoro Toshkent\n\nBuyruqlar ro'yxati: /help",
        "auth_failed": "❌ Login yoki parol noto'g'ri. Qayta urinib ko'ring.\nLoginni kiriting:",
        "logout": "👋 Akkauntdan chiqdingiz. Qayta kirish uchun /start bosing.",
        "not_authorized": "❌ Siz avtorizatsiyadan o'tmagansiz. Kirish uchun /start bosing.",
        "internal_error": "⚠️ Ichki xatolik. Birozdan keyin urinib ko'ring.",
        # Search
        "route_format": "❗ Format: &lt;chiqish shahri&gt; &lt;yetib borish shahri&gt;\nMisollar:\n  Buxoro Toshkent\n  Buxoro - Toshkent\n  Buxoro ➜ Toshkent",
        "searching": "🔎 Bugungi yuklarni qidiryapman: {from_city} ➜ {to_city}...",
        "search_error": "⚠️ Qidiruvda xatolik yuz berdi. Birozdan keyin urinib ko'ring.",
        "not_found": "❌ {from_city} ➜ {to_city} bo'yicha bugun yuk topilmadi.",
        "not_found_with_filters": "❌ {from_city} ➜ {to_city} bo'yicha (filtrlar bilan) bugun yuk topilmadi.\nFiltrlarni o'zgartirib ko'ring: /filters",
        "all_for_today": "ℹ️ Bugun shu yuklarning barchasi.\nMarshrutingiz bo'yicha yangi yuk paydo bo'lsa — darhol xabar beraman.",
        "cargo_found": "🚛 Yuk topildi",
        "new_cargo": "✅ Marshrutingiz bo'yicha yangi yuk paydo bo'ldi",
        "phone_label": "📞 Telefon",
        "sender_label": "👤 Yuborgan",
        "source_label": "📍 Manba",
        # Help
        "help_user": (
            "ℹ️ <b>Asosiy buyruqlar</b>\n"
            "/start — kirish\n"
            "/logout — chiqish\n"
            "/lang — tilni o'zgartirish\n"
            "/help — yordam\n\n"
            "📍 <b>Marshrut</b>\n"
            "/myroute — joriy marshrut\n"
            "/clearroute — marshrutni tozalash\n"
            "/notify on|off — bildirishnomalarni boshqarish\n\n"
            "🎯 <b>Filtrlar</b>\n"
            "/filters — joriy filtrlarni ko'rish\n"
            "/setfilter &lt;type&gt; &lt;value&gt; — filtr o'rnatish\n"
            "/clearfilters — filtrlarni tozalash\n\n"
            "📊 <b>Qo'shimcha</b>\n"
            "/stats — statistika\n"
            "/changepass &lt;yangi parol&gt; — parolni o'zgartirish\n\n"
            "Marshrut yuborish: shunchaki shahar nomlarini yozing, masalan:\n"
            "<code>Buxoro Toshkent</code>"
        ),
        "help_admin": (
            "\n\n🔧 <b>Admin: guruhlar</b>\n"
            "/groups — guruhlar ro'yxati\n"
            "/addgroup &lt;username&gt; — guruh qo'shish\n"
            "/delgroup &lt;username&gt; — guruhni o'chirish\n\n"
            "🔧 <b>Admin: foydalanuvchilar</b>\n"
            "/accounts — login akkauntlar ro'yxati\n"
            "/adduser &lt;login&gt; &lt;parol&gt; [admin] — yangi akkaunt yaratish\n"
            "/deluser &lt;login&gt; — akkauntni o'chirish\n"
            "/resetpass &lt;login&gt; &lt;yangi parol&gt; — parolni almashtirish\n"
            "/makeadmin &lt;login&gt; — adminga aylantirish\n"
            "/unadmin &lt;login&gt; — admin huquqini olib tashlash\n"
            "/users — foydalanuvchilar statistikasi\n"
            "/broadcast &lt;text&gt; — barcha foydalanuvchilarga xabar"
        ),
        # Admin: groups
        "admin_only": "❌ Bu buyruq faqat admin uchun.",
        "groups_empty": "📋 Guruhlar ro'yxati bo'sh.\nQo'shish uchun: /addgroup &lt;username&gt;",
        "groups_header": "📋 Monitor qilinayotgan guruhlar ({count}):",
        "group_added": "✅ Guruh qo'shildi: {name}",
        "group_removed": "✅ Guruh olib tashlandi: {name}",
        "group_not_found": "❌ Guruh ro'yxatda topilmadi: {name}",
        "group_already": "ℹ️ Guruh allaqachon ro'yxatda: {name}",
        "group_invalid": "❌ Username noto'g'ri formatda: {name}",
        "group_access_error": "❌ Guruhga ulanib bo'lmadi: {name}\nXato: {error}\n\nIltimos, akkauntingiz guruh a'zosi ekanligiga ishonch hosil qiling.",
        "addgroup_usage": "Foydalanish: /addgroup &lt;username&gt;\nMisol: /addgroup yuk95",
        "delgroup_usage": "Foydalanish: /delgroup &lt;username&gt;\nMisol: /delgroup yuk95",
        # Route management
        "myroute_none": "📍 Marshrut o'rnatilmagan.\n\nMarshrut yuborish uchun: shahar nomlarini yozing, masalan:\nBuxoro Toshkent",
        "myroute_show": "📍 <b>Joriy marshrut</b>\n{from_city} ➜ {to_city}\n\n🔔 Bildirishnomalar: <b>{notify}</b>",
        "clearroute_done": "✅ Marshrut tozalandi. Bildirishnomalar to'xtatildi.",
        # Auto-routes (multi-route subscriptions)
        "routes_list_header": "🔔 <b>Avto-qidiruvlar ({count})</b>\n\nBu marshrutlar bo'yicha bot tinmasdan kuzatadi va yangi yuk paydo bo'lganida sizga darhol xabar yuboradi.",
        "routes_list_empty": "🔔 Sizda avto-qidiruv yo'q.\n\nMarshrut qo'shish uchun pastdagi <b>➕ Yangi marshrut</b> tugmasini bosing yoki shunchaki yuk qidirish orqali avtomatik qo'shilsin.",
        "ibtn_add_route": "➕ Yangi marshrut",
        "form_enter_new_route": "🔎 Avto-qidiruv uchun marshrut kiriting:\n<code>Buxoro Toshkent</code>\n<code>Москва Ташкент</code>\n<code>Buxoro - Toshkent</code>",
        "route_added": "✅ Avto-qidiruv qo'shildi: <b>{from_city} ➜ {to_city}</b>\n\n🔔 Yangi yuk paydo bo'lganida darhol sizga xabar beriladi.",
        "route_already_exists": "ℹ️ Bu marshrut allaqachon ro'yxatda.",
        "route_deleted": "✅ Avto-qidiruv o'chirildi.",
        "route_invalid_format": "❗ Format noto'g'ri. Misol: <code>Buxoro Toshkent</code>",
        "notify_on": "🔔 Bildirishnomalar yoqildi.",
        "notify_off": "🔕 Bildirishnomalar o'chirildi.",
        "notify_usage": "Foydalanish: /notify on yoki /notify off",
        "notify_status_on": "yoqilgan",
        "notify_status_off": "o'chirilgan",
        # Filters
        "filters_none": "📊 Filtrlar o'rnatilmagan.\n\nFiltr o'rnatish uchun: /setfilter &lt;type&gt; &lt;value&gt;\n\nMavjud turlar:\n• <code>weight</code> — og'irlik (tonna)\n• <code>truck</code> — mashina turi\n• <code>phone</code> — telefon shart bo'lishi\n• <code>include</code> — kerak so'zlar\n• <code>exclude</code> — istisno so'zlar\n\nMisollar:\n<code>/setfilter weight 5-25</code>\n<code>/setfilter truck тент</code>\n<code>/setfilter phone on</code>\n<code>/setfilter include мука,рис</code>\n<code>/setfilter exclude отказ</code>",
        "filters_show_header": "📊 <b>Sizning filtrlaringiz</b>\n",
        "filter_weight_set": "⚖️ Og'irlik: <b>{value}</b> t",
        "filter_truck_set": "🚛 Mashina turi: <b>{value}</b>",
        "filter_phone_required": "📞 Telefon shart: <b>{value}</b>",
        "filter_include_set": "✅ Kerak so'zlar: <b>{value}</b>",
        "filter_exclude_set": "❌ Istisno so'zlar: <b>{value}</b>",
        "filter_yes": "ha",
        "filter_no": "yo'q",
        "filters_footer": "\nO'zgartirish: /setfilter &lt;type&gt; &lt;value&gt;\nTozalash: /clearfilters",
        "setfilter_usage": "Foydalanish: /setfilter &lt;type&gt; &lt;value&gt;\n\nTurlari: weight, truck, phone, include, exclude\n\nMisollar:\n<code>/setfilter weight 5-25</code>\n<code>/setfilter weight clear</code>\n<code>/setfilter truck тент</code>\n<code>/setfilter phone on</code>\n<code>/setfilter include мука,рис</code>",
        "setfilter_unknown": "❌ Noma'lum filtr turi: {type}\n\nMavjud: weight, truck, phone, include, exclude",
        "filter_weight_usage": "⚖️ Og'irlik filtri:\n<code>/setfilter weight 5-25</code> — diapazon\n<code>/setfilter weight min 5</code> — faqat min\n<code>/setfilter weight max 25</code> — faqat max\n<code>/setfilter weight clear</code> — tozalash",
        "filter_phone_usage": "📞 <code>/setfilter phone on</code> yoki <code>/setfilter phone off</code>",
        "filter_updated": "✅ Filtr yangilandi.",
        "filter_cleared": "✅ Filtr tozalandi.",
        "clearfilters_done": "✅ Barcha filtrlar tozalandi.",
        # Stats
        "stats_user": (
            "📊 <b>Sizning statistikangiz</b>\n\n"
            "🔎 Qidiruvlar: <b>{searches}</b>\n"
            "🚛 Olingan yuklar: <b>{cargos}</b>\n"
            "📍 Marshrut: <b>{route}</b>\n"
            "🔔 Bildirishnomalar: <b>{notify}</b>\n"
            "🌐 Til: <b>{language}</b>"
        ),
        "stats_no_route": "o'rnatilmagan",
        # Admin: users
        "admin_users": (
            "👥 <b>Foydalanuvchilar statistikasi</b>\n\n"
            "Jami: <b>{total}</b>\n"
            "Avtorizatsiyadan o'tgan: <b>{authorized}</b>\n"
            "Marshrut o'rnatilgan: <b>{with_routes}</b>\n"
            "Faol (24 soat): <b>{active_24h}</b>\n\n"
            "📋 Guruhlar: <b>{groups}</b>"
        ),
        # Broadcast
        "broadcast_usage": "Foydalanish: /broadcast &lt;xabar matni&gt;",
        "broadcast_done": "📢 Yuborildi: {sent} muvaffaqiyatli, {failed} xato.",
        "broadcast_received": "📢 <b>E'lon</b>\n\n{message}",
        # Account management (admin)
        "accounts_empty": "👥 Akkauntlar topilmadi.",
        "accounts_header": "👥 <b>Login akkauntlar ({count})</b>\n",
        "accounts_admin_label": "admin",
        "accounts_session_label": "sessiyalar",
        "accounts_routes_label": "marshrutlar",
        "accounts_footer": "\nQo'shish: /adduser &lt;login&gt; &lt;parol&gt; [admin]\nO'chirish: /deluser &lt;login&gt;\nParol: /resetpass &lt;login&gt; &lt;parol&gt;",
        "adduser_usage": "Foydalanish: /adduser &lt;login&gt; &lt;parol&gt; [admin]\n\nMisol:\n<code>/adduser ali 12345</code>\n<code>/adduser dilshod 12345 admin</code>\n\nLogin: 3-32 belgi (a-z, 0-9, _)\nParol: kamida 4 ta belgi",
        "adduser_invalid_login": "❌ Login formati noto'g'ri: <code>{login}</code>\nFaqat lotin harflari, raqam va _, 3-32 ta belgi.",
        "adduser_password_short": "❌ Parol juda qisqa. Kamida 4 ta belgi bo'lishi kerak.",
        "adduser_already_exists": "❌ Bu login allaqachon mavjud: <code>{login}</code>",
        "adduser_done": "✅ Akkaunt yaratildi:\n👤 Login: <code>{login}</code>\n🔑 Parol: <code>{password}</code>\n👑 Admin: <b>{admin}</b>\n\n⚠️ <b>Xavfsizlik uchun bu xabarni o'chirib qo'ying!</b>",
        "deluser_usage": "Foydalanish: /deluser &lt;login&gt;\nMisol: /deluser ali",
        "deluser_not_found": "❌ Login topilmadi: <code>{login}</code>",
        "deluser_self": "❌ O'zingizning akkauntingizni o'chira olmaysiz.",
        "deluser_last_admin": "❌ Oxirgi adminni o'chirib bo'lmaydi. Avval boshqa adminni yarating.",
        "deluser_done": "✅ Akkaunt o'chirildi: <code>{login}</code>\nFaol sessiyalar to'xtatildi.",
        "resetpass_usage": "Foydalanish: /resetpass &lt;login&gt; &lt;yangi parol&gt;\nMisol: /resetpass ali yangi12345",
        "resetpass_done": "✅ Parol yangilandi: <code>{login}</code>\n🔑 Yangi parol: <code>{password}</code>\n\n⚠️ Xavfsizlik uchun bu xabarni o'chirib qo'ying.",
        "changepass_usage": "Foydalanish: /changepass &lt;yangi parol&gt;\nMisol: <code>/changepass yangi12345</code>",
        "changepass_short": "❌ Parol juda qisqa. Kamida 4 ta belgi.",
        "changepass_done": "✅ Parolingiz o'zgartirildi.\n\n⚠️ Xavfsizlik uchun bu xabarni o'chirib qo'ying.",
        "makeadmin_usage": "Foydalanish: /makeadmin &lt;login&gt;",
        "unadmin_usage": "Foydalanish: /unadmin &lt;login&gt;",
        "makeadmin_done": "✅ Endi admin: <code>{login}</code>",
        "unadmin_done": "✅ Admin huquqi olindi: <code>{login}</code>",
        "unadmin_last": "❌ Oxirgi adminning huquqini olib bo'lmaydi.",
        "unadmin_self": "❌ O'zingizdan admin huquqini olib bo'lmaydi.",
        # Reply keyboard buttons
        "btn_search": "🔎 Yuk qidirish",
        "btn_route": "📍 Marshrut",
        "btn_filters": "🎯 Filtrlar",
        "btn_stats": "📊 Statistika",
        "btn_settings": "⚙️ Sozlamalar",
        "btn_help": "ℹ️ Yordam",
        "btn_admin": "🔧 Admin panel",
        "btn_back": "🔙 Orqaga",
        # Inline buttons / callbacks
        "ibtn_change_route": "📝 O'zgartirish",
        "ibtn_clear_route": "🗑 Tozalash",
        "ibtn_notify_on": "🔔 Bildirishnomalarni yoqish",
        "ibtn_notify_off": "🔕 Bildirishnomalarni o'chirish",
        "ibtn_back": "🔙 Orqaga",
        "ibtn_set_weight": "⚖️ Og'irlik",
        "ibtn_set_truck": "🚛 Mashina turi",
        "ibtn_set_phone_req": "📞 Telefon shart",
        "ibtn_set_include": "✅ Kerak so'zlar",
        "ibtn_set_exclude": "❌ Istisno so'zlar",
        "ibtn_clear_filters": "🗑 Hammasini tozalash",
        "ibtn_change_lang": "🌐 Tilni o'zgartirish",
        "ibtn_change_pass": "🔑 Parolni o'zgartirish",
        "ibtn_logout": "🚪 Chiqish",
        "ibtn_admin_users": "👥 Foydalanuvchilar",
        "ibtn_admin_groups": "📋 Guruhlar",
        "ibtn_admin_stats": "📊 Statistika",
        "ibtn_admin_broadcast": "📢 E'lon yuborish",
        "ibtn_admin_pending": "🆕 Yangi foydalanuvchilar ({count})",
        "ibtn_add_account": "➕ Yangi akkaunt",
        "ibtn_add_group": "➕ Guruh qo'shish",
        "ibtn_make_admin": "👑 Adminga aylantirish",
        "ibtn_unmake_admin": "👤 Admindan chiqarish",
        "ibtn_delete": "🗑 O'chirish",
        "ibtn_reset_pass": "🔑 Parolni almashtirish",
        "ibtn_approve": "✅ Tasdiqlash",
        "ibtn_reject": "🚫 Rad etish",
        "ibtn_yes": "✅ Ha",
        "ibtn_no": "❌ Yo'q",
        "ibtn_cancel": "❌ Bekor qilish",
        # Settings / panels
        "main_menu_hint": "Quyidagi tugmalardan foydalaning yoki marshrut yuboring (masalan: <i>Buxoro Toshkent</i>):",
        "settings_panel": "⚙️ <b>Sozlamalar</b>",
        "admin_panel": "🔧 <b>Admin panel</b>",
        "admin_pending_empty": "🆕 Kutilayotgan foydalanuvchi yo'q.",
        "admin_pending_header": "🆕 <b>Avtorizatsiyasiz foydalanuvchilar ({count})</b>\n\nUlar botni ishga tushirgan, lekin login/parol kiritmagan. Tasdiqlasangiz, bot ularga avtomatik akkaunt yaratib yuboradi.",
        "admin_pending_item": "👤 <b>{name}</b>\n  ID: <code>{chat_id}</code>\n  Boshlangan: {started}",
        "admin_user_actions": "👤 <b>{login}</b>\n  👑 Admin: {is_admin}\n  🔑 Parol: <code>{password}</code>\n  💬 Sessiyalar: <b>{sessions}</b>\n  📍 Marshrutlar: <b>{routes}</b>",
        "admin_group_actions": "📋 <b>@{username}</b>\n  Nomi: {title}\n  Qo'shilgan: {added_at}",
        "pending_user_first_seen": "👋 Salom! Botdan foydalanish uchun avtorizatsiya kerak.\n\nSizning so'rovingiz adminga yuborildi. Tasdiqlanganidan keyin sizga login va parol yuboriladi.",
        "admin_new_pending_notification": "🆕 <b>Yangi foydalanuvchi botni boshladi</b>\n\n👤 {name}\n🆔 <code>{chat_id}</code>\n\n/admin → Yangi foydalanuvchilar — tasdiqlash uchun",
        "credentials_sent_to_user": "✅ Akkaunt yaratildi va foydalanuvchiga yuborildi.\nLogin: <code>{login}</code>",
        "user_received_credentials": "✅ <b>Akkauntingiz yaratildi!</b>\n\n👤 Login: <code>{login}</code>\n🔑 Parol: <code>{password}</code>\n\n⚠️ Xavfsizlik uchun parolni eslab qoling va bu xabarni o'chirib qo'ying.\n\nKirish uchun /start bosing.",
        "pending_rejected": "🚫 Foydalanuvchi rad etildi.",
        "pending_user_was_rejected": "🚫 Sizning kirish so'rovingiz admin tomonidan rad etildi.",
        "form_enter_login": "Yangi login kiriting (3-32 ta belgi: a-z, 0-9, _):",
        "form_enter_password": "Yangi parolni kiriting (kamida 4 ta belgi):",
        "form_confirm_admin": "Bu foydalanuvchi admin bo'lsinmi?",
        "form_enter_route": "Yangi marshrutni yuboring, masalan:\n<code>Buxoro Toshkent</code>",
        "form_enter_truck": "Mashina turini kiriting (masalan: тент, реф):\n(yoki \"clear\" — tozalash uchun)",
        "form_enter_weight": "Og'irlik diapazonini kiriting:\n<code>5-25</code> — diapazon\n<code>min 5</code> — faqat min\n<code>max 25</code> — faqat max\n<code>clear</code> — tozalash",
        "form_enter_include": "Kerak so'zlarni vergul bilan ajratib kiriting (masalan: <code>мука,рис</code>):\n(yoki \"clear\" — tozalash uchun)",
        "form_enter_exclude": "Istisno so'zlarni vergul bilan ajratib kiriting (masalan: <code>отказ,штраф</code>):\n(yoki \"clear\" — tozalash uchun)",
        "form_enter_group": "Guruh username yoki link yuboring:\n<code>yuk95</code>\n<code>@yuk95</code>\n<code>https://t.me/yuk95</code>",
        "form_enter_broadcast": "Yubormoqchi bo'lgan xabaringizni yuboring:",
        "form_enter_new_password": "Yangi parolni yuboring:",
        "form_enter_login_for_reset": "Qaysi loginning parolini almashtirmoqchisiz?",
        "form_cancelled": "❌ Bekor qilindi.",
        "search_prompt": "🔎 Marshrut yuboring (masalan: <code>Buxoro Toshkent</code>)",
        # Broadcast targeting
        "bcast_choose_target": "📢 Kimga yubormoqchisiz?",
        "bcast_to_all": "📢 Hammaga",
        "bcast_to_selected": "👤 Tanlanganlarga",
        "bcast_select_users": "👤 Foydalanuvchilarni tanlang ({count} tanlangan):",
        "bcast_no_selected": "❌ Bironta foydalanuvchi tanlanmadi.",
        "bcast_no_users": "❌ Avtorizatsiyadan o'tgan foydalanuvchilar yo'q.",
        "ibtn_bcast_all": "📢 Hammaga",
        "ibtn_bcast_select": "👤 Tanlash",
        "ibtn_select_all": "✅ Hammasini",
        "ibtn_clear_selection": "🗑 Tozalash",
        "ibtn_send_to_selected": "📤 Yuborish ({count})",
        # Phone capture
        "phone_request": "📱 Iltimos, telefon raqamingizni baham ko'ring (yoki o'tkazib yuboring):",
        "btn_share_phone": "📱 Telefon yuborish",
        "btn_skip_phone": "⏭ O'tkazib yuborish",
        "phone_received": "✅ Telefon saqlandi: {phone}",
        "phone_skipped": "ℹ️ Telefon yuborilmadi.",
        # Access-request panel (post-phone for unauthorized users)
        "access_no_permission": "ℹ️ Sizga hali kirish ruxsati berilmagan.\n\nAgar adminda akkauntingiz bo'lsa — login va parol bilan kiring.\nAks holda <b>📨 Adminga murojat qilish</b> tugmasini bosib, ruxsat so'rang.",
        "btn_request_login": "🔑 Loginim bor — kirish",
        "btn_request_message_admin": "📨 Adminga murojat qilish",
        "waiting_for_admin_reply": "⏳ Xabaringiz adminga yetkazildi.\nJavob kelguncha yana yozishingiz mumkin yoki loginingiz bor bo'lsa kirishingiz mumkin:",
        # Messages module
        "btn_msg_admin": "📨 Adminga xabar",
        "form_enter_message_to_admin": "✍️ Adminga yozmoqchi bo'lgan xabaringizni yuboring (matn, rasm yoki fayl):",
        "form_enter_reply_to_user": "✍️ Foydalanuvchiga javobingizni yozing (matn, rasm yoki fayl):",
        "message_sent_to_admin": "✅ Xabaringiz adminga yetkazildi.\nJavob kelganda sizga xabar beriladi.",
        "message_no_admins": "⚠️ Hozirda admin yo'q. Birozdan keyin urinib ko'ring.",
        "user_received_admin_reply": "📩 <b>Adminning javobi:</b>",
        "ibtn_admin_messages": "💬 Xabarlar ({count})",
        "ibtn_view_thread": "👁 Ko'rish",
        "ibtn_reply_to_user": "✍️ Javob yozish",
        "admin_messages_empty": "💬 Xabarlar yo'q.",
        "admin_messages_header": "💬 <b>Xabarlar ({count} suhbat, {unread} o'qilmagan)</b>",
        "admin_msg_thread_header": "💬 <b>Suhbat: {name}</b>",
        "admin_msg_thread_user_info": "👤 <b>{name}</b>\n{username}\n📞 {phone}\n🆔 <code>{chat_id}</code>",
        "admin_new_message_notification": "📨 <b>Yangi xabar:</b>\n👤 {name}\n📞 {phone}\n\n💬 {preview}",
        "admin_reply_sent": "✅ Javob foydalanuvchiga yuborildi.",
        "msg_user_label": "👤 Foydalanuvchi",
        "msg_admin_label": "👑 Admin ({login})",
        "unknown_user": "Noma'lum",
    },
    "ru": {
        # Language
        "choose_language": "🌐 Tilni tanlang / Выберите язык / Choose language:",
        "language_set": "✅ Язык установлен: Русский",
        "lang_prompt": "🌐 Выберите новый язык:",
        # Auth
        "welcome": "👋 Привет! Для использования бота требуется авторизация.",
        "enter_login": "Введите логин:",
        "enter_password": "Введите пароль:",
        "login_empty": "Логин не может быть пустым. Введите логин:",
        "auth_success": "✅ Авторизация успешна!\nТеперь вы можете искать грузы.\nВведите маршрут, например:\nБухара Ташкент\n\nСписок команд: /help",
        "auth_failed": "❌ Неверный логин или пароль. Попробуйте снова.\nВведите логин:",
        "logout": "👋 Вы вышли из аккаунта. Используйте /start для повторного входа.",
        "not_authorized": "❌ Вы не авторизованы. Используйте /start для входа.",
        "internal_error": "⚠️ Внутренняя ошибка. Попробуйте позже.",
        # Search
        "route_format": "❗ Формат: &lt;город отправления&gt; &lt;город назначения&gt;\nПримеры:\n  Бухара Ташкент\n  Бухара - Ташкент\n  Бухара ➜ Ташкент",
        "searching": "🔎 Ищу грузы {from_city} ➜ {to_city} за сегодня...",
        "search_error": "⚠️ Произошла ошибка при поиске. Попробуйте позже.",
        "not_found": "❌ По запросу {from_city} ➜ {to_city} грузов не найдено за сегодня.",
        "not_found_with_filters": "❌ По запросу {from_city} ➜ {to_city} (с фильтрами) грузов не найдено.\nПопробуйте изменить фильтры: /filters",
        "all_for_today": "ℹ️ Пока это все грузы за сегодня.\nЕсли появится новый груз по вашему маршруту — я сразу сообщу.",
        "cargo_found": "🚛 Груз найден",
        "new_cargo": "✅ Появился новый груз по вашему маршруту",
        "phone_label": "📞 Телефон",
        "sender_label": "👤 Отправил",
        "source_label": "📍 Источник",
        # Help
        "help_user": (
            "ℹ️ <b>Основные команды</b>\n"
            "/start — войти\n"
            "/logout — выйти\n"
            "/lang — изменить язык\n"
            "/help — помощь\n\n"
            "📍 <b>Маршрут</b>\n"
            "/myroute — текущий маршрут\n"
            "/clearroute — очистить маршрут\n"
            "/notify on|off — управление уведомлениями\n\n"
            "🎯 <b>Фильтры</b>\n"
            "/filters — текущие фильтры\n"
            "/setfilter &lt;type&gt; &lt;value&gt; — установить фильтр\n"
            "/clearfilters — очистить фильтры\n\n"
            "📊 <b>Дополнительно</b>\n"
            "/stats — статистика\n"
            "/changepass &lt;новый пароль&gt; — сменить пароль\n\n"
            "Чтобы искать груз — просто отправьте маршрут, например:\n"
            "<code>Бухара Ташкент</code>"
        ),
        "help_admin": (
            "\n\n🔧 <b>Админ: группы</b>\n"
            "/groups — список групп\n"
            "/addgroup &lt;username&gt; — добавить группу\n"
            "/delgroup &lt;username&gt; — удалить группу\n\n"
            "🔧 <b>Админ: пользователи</b>\n"
            "/accounts — список логинов\n"
            "/adduser &lt;login&gt; &lt;пароль&gt; [admin] — создать аккаунт\n"
            "/deluser &lt;login&gt; — удалить аккаунт\n"
            "/resetpass &lt;login&gt; &lt;новый пароль&gt; — сменить пароль\n"
            "/makeadmin &lt;login&gt; — сделать админом\n"
            "/unadmin &lt;login&gt; — снять админа\n"
            "/users — статистика пользователей\n"
            "/broadcast &lt;text&gt; — рассылка"
        ),
        # Admin: groups
        "admin_only": "❌ Эта команда только для админа.",
        "groups_empty": "📋 Список групп пуст.\nДобавить: /addgroup &lt;username&gt;",
        "groups_header": "📋 Мониторинг групп ({count}):",
        "group_added": "✅ Группа добавлена: {name}",
        "group_removed": "✅ Группа удалена: {name}",
        "group_not_found": "❌ Группа не найдена в списке: {name}",
        "group_already": "ℹ️ Группа уже в списке: {name}",
        "group_invalid": "❌ Неверный формат username: {name}",
        "group_access_error": "❌ Не удалось получить доступ к группе: {name}\nОшибка: {error}\n\nУбедитесь, что ваш аккаунт состоит в группе.",
        "addgroup_usage": "Использование: /addgroup &lt;username&gt;\nПример: /addgroup yuk95",
        "delgroup_usage": "Использование: /delgroup &lt;username&gt;\nПример: /delgroup yuk95",
        # Route management
        "myroute_none": "📍 Маршрут не установлен.\n\nЧтобы установить — отправьте города, например:\nБухара Ташкент",
        "myroute_show": "📍 <b>Текущий маршрут</b>\n{from_city} ➜ {to_city}\n\n🔔 Уведомления: <b>{notify}</b>",
        "clearroute_done": "✅ Маршрут очищен. Уведомления остановлены.",
        # Auto-routes (multi-route subscriptions)
        "routes_list_header": "🔔 <b>Авто-поиск ({count})</b>\n\nПо этим маршрутам бот следит непрерывно и сразу уведомит, когда появится новый груз.",
        "routes_list_empty": "🔔 У вас нет активных авто-поисков.\n\nЧтобы добавить — нажмите <b>➕ Новый маршрут</b> ниже или просто выполните поиск (маршрут добавится автоматически).",
        "ibtn_add_route": "➕ Новый маршрут",
        "form_enter_new_route": "🔎 Введите маршрут для авто-поиска:\n<code>Бухара Ташкент</code>\n<code>Buxoro Toshkent</code>\n<code>Бухара - Ташкент</code>",
        "route_added": "✅ Авто-поиск добавлен: <b>{from_city} ➜ {to_city}</b>\n\n🔔 Когда появится новый груз — я сразу сообщу.",
        "route_already_exists": "ℹ️ Этот маршрут уже в списке.",
        "route_deleted": "✅ Авто-поиск удалён.",
        "route_invalid_format": "❗ Неверный формат. Пример: <code>Бухара Ташкент</code>",
        "notify_on": "🔔 Уведомления включены.",
        "notify_off": "🔕 Уведомления отключены.",
        "notify_usage": "Использование: /notify on или /notify off",
        "notify_status_on": "включены",
        "notify_status_off": "отключены",
        # Filters
        "filters_none": "📊 Фильтры не установлены.\n\nЧтобы установить: /setfilter &lt;type&gt; &lt;value&gt;\n\nДоступные типы:\n• <code>weight</code> — вес (тонн)\n• <code>truck</code> — тип машины\n• <code>phone</code> — обязательный телефон\n• <code>include</code> — обязательные слова\n• <code>exclude</code> — исключающие слова\n\nПримеры:\n<code>/setfilter weight 5-25</code>\n<code>/setfilter truck тент</code>\n<code>/setfilter phone on</code>\n<code>/setfilter include мука,рис</code>\n<code>/setfilter exclude отказ</code>",
        "filters_show_header": "📊 <b>Ваши фильтры</b>\n",
        "filter_weight_set": "⚖️ Вес: <b>{value}</b> т",
        "filter_truck_set": "🚛 Тип машины: <b>{value}</b>",
        "filter_phone_required": "📞 Телефон обязателен: <b>{value}</b>",
        "filter_include_set": "✅ Обязательные слова: <b>{value}</b>",
        "filter_exclude_set": "❌ Исключающие слова: <b>{value}</b>",
        "filter_yes": "да",
        "filter_no": "нет",
        "filters_footer": "\nИзменить: /setfilter &lt;type&gt; &lt;value&gt;\nОчистить: /clearfilters",
        "setfilter_usage": "Использование: /setfilter &lt;type&gt; &lt;value&gt;\n\nТипы: weight, truck, phone, include, exclude\n\nПримеры:\n<code>/setfilter weight 5-25</code>\n<code>/setfilter weight clear</code>\n<code>/setfilter truck тент</code>\n<code>/setfilter phone on</code>\n<code>/setfilter include мука,рис</code>",
        "setfilter_unknown": "❌ Неизвестный тип фильтра: {type}\n\nДоступны: weight, truck, phone, include, exclude",
        "filter_weight_usage": "⚖️ Фильтр веса:\n<code>/setfilter weight 5-25</code> — диапазон\n<code>/setfilter weight min 5</code> — только минимум\n<code>/setfilter weight max 25</code> — только максимум\n<code>/setfilter weight clear</code> — очистить",
        "filter_phone_usage": "📞 <code>/setfilter phone on</code> или <code>/setfilter phone off</code>",
        "filter_updated": "✅ Фильтр обновлён.",
        "filter_cleared": "✅ Фильтр очищен.",
        "clearfilters_done": "✅ Все фильтры очищены.",
        # Stats
        "stats_user": (
            "📊 <b>Ваша статистика</b>\n\n"
            "🔎 Поисков: <b>{searches}</b>\n"
            "🚛 Получено грузов: <b>{cargos}</b>\n"
            "📍 Маршрут: <b>{route}</b>\n"
            "🔔 Уведомления: <b>{notify}</b>\n"
            "🌐 Язык: <b>{language}</b>"
        ),
        "stats_no_route": "не установлен",
        # Admin: users
        "admin_users": (
            "👥 <b>Статистика пользователей</b>\n\n"
            "Всего: <b>{total}</b>\n"
            "Авторизованных: <b>{authorized}</b>\n"
            "С маршрутами: <b>{with_routes}</b>\n"
            "Активны (24 ч): <b>{active_24h}</b>\n\n"
            "📋 Групп: <b>{groups}</b>"
        ),
        # Broadcast
        "broadcast_usage": "Использование: /broadcast &lt;текст сообщения&gt;",
        "broadcast_done": "📢 Отправлено: {sent} успешно, {failed} с ошибкой.",
        "broadcast_received": "📢 <b>Объявление</b>\n\n{message}",
        # Account management (admin)
        "accounts_empty": "👥 Аккаунтов нет.",
        "accounts_header": "👥 <b>Логины ({count})</b>\n",
        "accounts_admin_label": "админ",
        "accounts_session_label": "сессий",
        "accounts_routes_label": "маршрутов",
        "accounts_footer": "\nДобавить: /adduser &lt;login&gt; &lt;пароль&gt; [admin]\nУдалить: /deluser &lt;login&gt;\nПароль: /resetpass &lt;login&gt; &lt;пароль&gt;",
        "adduser_usage": "Использование: /adduser &lt;login&gt; &lt;пароль&gt; [admin]\n\nПример:\n<code>/adduser ali 12345</code>\n<code>/adduser dilshod 12345 admin</code>\n\nЛогин: 3-32 символа (a-z, 0-9, _)\nПароль: минимум 4 символа",
        "adduser_invalid_login": "❌ Неверный формат логина: <code>{login}</code>\nТолько латиница, цифры и _, 3-32 символа.",
        "adduser_password_short": "❌ Пароль слишком короткий. Минимум 4 символа.",
        "adduser_already_exists": "❌ Логин уже существует: <code>{login}</code>",
        "adduser_done": "✅ Аккаунт создан:\n👤 Логин: <code>{login}</code>\n🔑 Пароль: <code>{password}</code>\n👑 Админ: <b>{admin}</b>\n\n⚠️ <b>Удалите это сообщение для безопасности!</b>",
        "deluser_usage": "Использование: /deluser &lt;login&gt;\nПример: /deluser ali",
        "deluser_not_found": "❌ Логин не найден: <code>{login}</code>",
        "deluser_self": "❌ Нельзя удалить свой собственный аккаунт.",
        "deluser_last_admin": "❌ Нельзя удалить последнего админа. Сначала создайте другого.",
        "deluser_done": "✅ Аккаунт удалён: <code>{login}</code>\nАктивные сессии остановлены.",
        "resetpass_usage": "Использование: /resetpass &lt;login&gt; &lt;новый пароль&gt;\nПример: /resetpass ali new12345",
        "resetpass_done": "✅ Пароль обновлён: <code>{login}</code>\n🔑 Новый пароль: <code>{password}</code>\n\n⚠️ Удалите это сообщение для безопасности.",
        "changepass_usage": "Использование: /changepass &lt;новый пароль&gt;\nПример: <code>/changepass new12345</code>",
        "changepass_short": "❌ Пароль слишком короткий. Минимум 4 символа.",
        "changepass_done": "✅ Пароль изменён.\n\n⚠️ Удалите это сообщение для безопасности.",
        "makeadmin_usage": "Использование: /makeadmin &lt;login&gt;",
        "unadmin_usage": "Использование: /unadmin &lt;login&gt;",
        "makeadmin_done": "✅ Теперь админ: <code>{login}</code>",
        "unadmin_done": "✅ Снят с админа: <code>{login}</code>",
        "unadmin_last": "❌ Нельзя снять последнего админа.",
        "unadmin_self": "❌ Нельзя снять админа с себя.",
        # Reply keyboard buttons
        "btn_search": "🔎 Поиск груза",
        "btn_route": "📍 Маршрут",
        "btn_filters": "🎯 Фильтры",
        "btn_stats": "📊 Статистика",
        "btn_settings": "⚙️ Настройки",
        "btn_help": "ℹ️ Помощь",
        "btn_admin": "🔧 Админ панель",
        "btn_back": "🔙 Назад",
        # Inline buttons / callbacks
        "ibtn_change_route": "📝 Изменить",
        "ibtn_clear_route": "🗑 Очистить",
        "ibtn_notify_on": "🔔 Включить уведомления",
        "ibtn_notify_off": "🔕 Выключить уведомления",
        "ibtn_back": "🔙 Назад",
        "ibtn_set_weight": "⚖️ Вес",
        "ibtn_set_truck": "🚛 Тип машины",
        "ibtn_set_phone_req": "📞 Телефон обязателен",
        "ibtn_set_include": "✅ Обязательные слова",
        "ibtn_set_exclude": "❌ Исключающие слова",
        "ibtn_clear_filters": "🗑 Очистить все",
        "ibtn_change_lang": "🌐 Сменить язык",
        "ibtn_change_pass": "🔑 Сменить пароль",
        "ibtn_logout": "🚪 Выйти",
        "ibtn_admin_users": "👥 Пользователи",
        "ibtn_admin_groups": "📋 Группы",
        "ibtn_admin_stats": "📊 Статистика",
        "ibtn_admin_broadcast": "📢 Рассылка",
        "ibtn_admin_pending": "🆕 Новые пользователи ({count})",
        "ibtn_add_account": "➕ Новый аккаунт",
        "ibtn_add_group": "➕ Добавить группу",
        "ibtn_make_admin": "👑 Сделать админом",
        "ibtn_unmake_admin": "👤 Снять админа",
        "ibtn_delete": "🗑 Удалить",
        "ibtn_reset_pass": "🔑 Сменить пароль",
        "ibtn_approve": "✅ Одобрить",
        "ibtn_reject": "🚫 Отклонить",
        "ibtn_yes": "✅ Да",
        "ibtn_no": "❌ Нет",
        "ibtn_cancel": "❌ Отмена",
        # Settings / panels
        "main_menu_hint": "Используйте кнопки ниже или отправьте маршрут (например: <i>Бухара Ташкент</i>):",
        "settings_panel": "⚙️ <b>Настройки</b>",
        "admin_panel": "🔧 <b>Админ панель</b>",
        "admin_pending_empty": "🆕 Ожидающих пользователей нет.",
        "admin_pending_header": "🆕 <b>Неавторизованные пользователи ({count})</b>\n\nОни запустили бота, но не вошли. После одобрения бот автоматически создаст им аккаунт и пришлёт логин/пароль.",
        "admin_pending_item": "👤 <b>{name}</b>\n  ID: <code>{chat_id}</code>\n  Начал: {started}",
        "admin_user_actions": "👤 <b>{login}</b>\n  👑 Админ: {is_admin}\n  🔑 Пароль: <code>{password}</code>\n  💬 Сессии: <b>{sessions}</b>\n  📍 Маршруты: <b>{routes}</b>",
        "admin_group_actions": "📋 <b>@{username}</b>\n  Название: {title}\n  Добавлена: {added_at}",
        "pending_user_first_seen": "👋 Привет! Для использования бота требуется авторизация.\n\nВаш запрос отправлен админу. После одобрения вы получите логин и пароль.",
        "admin_new_pending_notification": "🆕 <b>Новый пользователь запустил бота</b>\n\n👤 {name}\n🆔 <code>{chat_id}</code>\n\n/admin → Новые пользователи — для одобрения",
        "credentials_sent_to_user": "✅ Аккаунт создан и отправлен пользователю.\nЛогин: <code>{login}</code>",
        "user_received_credentials": "✅ <b>Ваш аккаунт создан!</b>\n\n👤 Логин: <code>{login}</code>\n🔑 Пароль: <code>{password}</code>\n\n⚠️ Запомните пароль и удалите это сообщение для безопасности.\n\nДля входа используйте /start.",
        "pending_rejected": "🚫 Пользователь отклонён.",
        "pending_user_was_rejected": "🚫 Ваш запрос на доступ был отклонён администратором.",
        "form_enter_login": "Введите новый логин (3-32 символа: a-z, 0-9, _):",
        "form_enter_password": "Введите новый пароль (минимум 4 символа):",
        "form_confirm_admin": "Сделать пользователя админом?",
        "form_enter_route": "Отправьте новый маршрут, например:\n<code>Бухара Ташкент</code>",
        "form_enter_truck": "Введите тип машины (например: тент, реф):\n(или \"clear\" — для очистки)",
        "form_enter_weight": "Введите диапазон веса:\n<code>5-25</code> — диапазон\n<code>min 5</code> — только минимум\n<code>max 25</code> — только максимум\n<code>clear</code> — очистить",
        "form_enter_include": "Введите обязательные слова через запятую (например: <code>мука,рис</code>):\n(или \"clear\" — для очистки)",
        "form_enter_exclude": "Введите исключающие слова через запятую (например: <code>отказ,штраф</code>):\n(или \"clear\" — для очистки)",
        "form_enter_group": "Отправьте username группы или ссылку:\n<code>yuk95</code>\n<code>@yuk95</code>\n<code>https://t.me/yuk95</code>",
        "form_enter_broadcast": "Отправьте текст рассылки:",
        "form_enter_new_password": "Отправьте новый пароль:",
        "form_enter_login_for_reset": "Для какого логина сменить пароль?",
        "form_cancelled": "❌ Отменено.",
        "search_prompt": "🔎 Отправьте маршрут (например: <code>Бухара Ташкент</code>)",
        # Broadcast targeting
        "bcast_choose_target": "📢 Кому отправить?",
        "bcast_to_all": "📢 Всем",
        "bcast_to_selected": "👤 Выбранным",
        "bcast_select_users": "👤 Выберите пользователей ({count} выбрано):",
        "bcast_no_selected": "❌ Никто не выбран.",
        "bcast_no_users": "❌ Нет авторизованных пользователей.",
        "ibtn_bcast_all": "📢 Всем",
        "ibtn_bcast_select": "👤 Выбрать",
        "ibtn_select_all": "✅ Все",
        "ibtn_clear_selection": "🗑 Очистить",
        "ibtn_send_to_selected": "📤 Отправить ({count})",
        # Phone capture
        "phone_request": "📱 Пожалуйста, поделитесь номером телефона (или пропустите):",
        "btn_share_phone": "📱 Отправить телефон",
        "btn_skip_phone": "⏭ Пропустить",
        "phone_received": "✅ Телефон сохранён: {phone}",
        "phone_skipped": "ℹ️ Телефон не отправлен.",
        # Access-request panel (post-phone for unauthorized users)
        "access_no_permission": "ℹ️ Вам ещё не предоставлен доступ.\n\nЕсли у вас есть аккаунт от админа — войдите по логину и паролю.\nИначе нажмите <b>📨 Написать админу</b> и попросите выдать доступ.",
        "btn_request_login": "🔑 У меня есть логин — войти",
        "btn_request_message_admin": "📨 Написать админу",
        "waiting_for_admin_reply": "⏳ Ваше сообщение отправлено админу.\nМожно отправить ещё одно или войти, если есть логин:",
        # Messages module
        "btn_msg_admin": "📨 Написать админу",
        "form_enter_message_to_admin": "✍️ Отправьте ваше сообщение админу (текст, фото или файл):",
        "form_enter_reply_to_user": "✍️ Напишите ответ пользователю (текст, фото или файл):",
        "message_sent_to_admin": "✅ Сообщение отправлено админу.\nКогда придёт ответ, я сообщу.",
        "message_no_admins": "⚠️ Сейчас админов нет. Попробуйте позже.",
        "user_received_admin_reply": "📩 <b>Ответ админа:</b>",
        "ibtn_admin_messages": "💬 Сообщения ({count})",
        "ibtn_view_thread": "👁 Открыть",
        "ibtn_reply_to_user": "✍️ Ответить",
        "admin_messages_empty": "💬 Сообщений нет.",
        "admin_messages_header": "💬 <b>Сообщения ({count} диалогов, {unread} непрочитанных)</b>",
        "admin_msg_thread_header": "💬 <b>Диалог: {name}</b>",
        "admin_msg_thread_user_info": "👤 <b>{name}</b>\n{username}\n📞 {phone}\n🆔 <code>{chat_id}</code>",
        "admin_new_message_notification": "📨 <b>Новое сообщение:</b>\n👤 {name}\n📞 {phone}\n\n💬 {preview}",
        "admin_reply_sent": "✅ Ответ отправлен пользователю.",
        "msg_user_label": "👤 Пользователь",
        "msg_admin_label": "👑 Админ ({login})",
        "unknown_user": "Неизвестно",
    },
    "en": {
        # Language
        "choose_language": "🌐 Tilni tanlang / Выберите язык / Choose language:",
        "language_set": "✅ Language set: English",
        "lang_prompt": "🌐 Choose a new language:",
        # Auth
        "welcome": "👋 Hello! Authorization is required to use the bot.",
        "enter_login": "Enter your login:",
        "enter_password": "Enter your password:",
        "login_empty": "Login cannot be empty. Enter your login:",
        "auth_success": "✅ Authorization successful!\nYou can now search for cargo.\nEnter a route, for example:\nBukhara Tashkent\n\nCommand list: /help",
        "auth_failed": "❌ Invalid login or password. Try again.\nEnter your login:",
        "logout": "👋 You have been logged out. Use /start to log in again.",
        "not_authorized": "❌ You are not authorized. Use /start to log in.",
        "internal_error": "⚠️ Internal error. Please try again later.",
        # Search
        "route_format": "❗ Format: &lt;departure city&gt; &lt;destination city&gt;\nExamples:\n  Bukhara Tashkent\n  Bukhara - Tashkent\n  Bukhara ➜ Tashkent",
        "searching": "🔎 Searching today's cargo: {from_city} ➜ {to_city}...",
        "search_error": "⚠️ Search error occurred. Please try again later.",
        "not_found": "❌ No cargo found for {from_city} ➜ {to_city} today.",
        "not_found_with_filters": "❌ No cargo found for {from_city} ➜ {to_city} (with filters) today.\nTry adjusting filters: /filters",
        "all_for_today": "ℹ️ That's all cargo for today.\nI'll notify you immediately when new cargo matching your route appears.",
        "cargo_found": "🚛 Cargo found",
        "new_cargo": "✅ New cargo on your route",
        "phone_label": "📞 Phone",
        "sender_label": "👤 Sender",
        "source_label": "📍 Source",
        # Help
        "help_user": (
            "ℹ️ <b>Main commands</b>\n"
            "/start — log in\n"
            "/logout — log out\n"
            "/lang — change language\n"
            "/help — help\n\n"
            "📍 <b>Route</b>\n"
            "/myroute — current route\n"
            "/clearroute — clear route\n"
            "/notify on|off — manage notifications\n\n"
            "🎯 <b>Filters</b>\n"
            "/filters — current filters\n"
            "/setfilter &lt;type&gt; &lt;value&gt; — set a filter\n"
            "/clearfilters — clear filters\n\n"
            "📊 <b>Other</b>\n"
            "/stats — statistics\n"
            "/changepass &lt;new password&gt; — change password\n\n"
            "To search cargo just send a route, e.g.:\n"
            "<code>Bukhara Tashkent</code>"
        ),
        "help_admin": (
            "\n\n🔧 <b>Admin: groups</b>\n"
            "/groups — list groups\n"
            "/addgroup &lt;username&gt; — add a group\n"
            "/delgroup &lt;username&gt; — remove a group\n\n"
            "🔧 <b>Admin: users</b>\n"
            "/accounts — list of logins\n"
            "/adduser &lt;login&gt; &lt;password&gt; [admin] — create account\n"
            "/deluser &lt;login&gt; — delete account\n"
            "/resetpass &lt;login&gt; &lt;new password&gt; — change password\n"
            "/makeadmin &lt;login&gt; — promote to admin\n"
            "/unadmin &lt;login&gt; — demote from admin\n"
            "/users — user statistics\n"
            "/broadcast &lt;text&gt; — send announcement"
        ),
        # Admin: groups
        "admin_only": "❌ This command is for admin only.",
        "groups_empty": "📋 Groups list is empty.\nAdd: /addgroup &lt;username&gt;",
        "groups_header": "📋 Monitored groups ({count}):",
        "group_added": "✅ Group added: {name}",
        "group_removed": "✅ Group removed: {name}",
        "group_not_found": "❌ Group not found in list: {name}",
        "group_already": "ℹ️ Group is already on the list: {name}",
        "group_invalid": "❌ Invalid username format: {name}",
        "group_access_error": "❌ Could not access the group: {name}\nError: {error}\n\nMake sure your account is a member of the group.",
        "addgroup_usage": "Usage: /addgroup &lt;username&gt;\nExample: /addgroup yuk95",
        "delgroup_usage": "Usage: /delgroup &lt;username&gt;\nExample: /delgroup yuk95",
        # Route management
        "myroute_none": "📍 No route set.\n\nTo set a route — just send city names, e.g.:\nBukhara Tashkent",
        "myroute_show": "📍 <b>Current route</b>\n{from_city} ➜ {to_city}\n\n🔔 Notifications: <b>{notify}</b>",
        "clearroute_done": "✅ Route cleared. Notifications stopped.",
        # Auto-routes (multi-route subscriptions)
        "routes_list_header": "🔔 <b>Auto-search routes ({count})</b>\n\nThe bot monitors these routes continuously and notifies you immediately when new cargo appears.",
        "routes_list_empty": "🔔 You have no auto-search routes.\n\nTap <b>➕ New route</b> below or simply do a search (it'll be added automatically).",
        "ibtn_add_route": "➕ New route",
        "form_enter_new_route": "🔎 Enter route for auto-search:\n<code>Bukhara Tashkent</code>\n<code>Москва Ташкент</code>\n<code>Bukhara - Tashkent</code>",
        "route_added": "✅ Auto-search added: <b>{from_city} ➜ {to_city}</b>\n\n🔔 You'll be notified the moment new cargo appears.",
        "route_already_exists": "ℹ️ This route is already in your list.",
        "route_deleted": "✅ Auto-search removed.",
        "route_invalid_format": "❗ Invalid format. Example: <code>Bukhara Tashkent</code>",
        "notify_on": "🔔 Notifications enabled.",
        "notify_off": "🔕 Notifications disabled.",
        "notify_usage": "Usage: /notify on or /notify off",
        "notify_status_on": "enabled",
        "notify_status_off": "disabled",
        # Filters
        "filters_none": "📊 No filters set.\n\nTo set: /setfilter &lt;type&gt; &lt;value&gt;\n\nAvailable types:\n• <code>weight</code> — weight (tons)\n• <code>truck</code> — truck type\n• <code>phone</code> — require phone\n• <code>include</code> — required keywords\n• <code>exclude</code> — excluded keywords\n\nExamples:\n<code>/setfilter weight 5-25</code>\n<code>/setfilter truck тент</code>\n<code>/setfilter phone on</code>\n<code>/setfilter include flour,rice</code>\n<code>/setfilter exclude rejected</code>",
        "filters_show_header": "📊 <b>Your filters</b>\n",
        "filter_weight_set": "⚖️ Weight: <b>{value}</b> t",
        "filter_truck_set": "🚛 Truck type: <b>{value}</b>",
        "filter_phone_required": "📞 Phone required: <b>{value}</b>",
        "filter_include_set": "✅ Required keywords: <b>{value}</b>",
        "filter_exclude_set": "❌ Excluded keywords: <b>{value}</b>",
        "filter_yes": "yes",
        "filter_no": "no",
        "filters_footer": "\nChange: /setfilter &lt;type&gt; &lt;value&gt;\nClear: /clearfilters",
        "setfilter_usage": "Usage: /setfilter &lt;type&gt; &lt;value&gt;\n\nTypes: weight, truck, phone, include, exclude\n\nExamples:\n<code>/setfilter weight 5-25</code>\n<code>/setfilter weight clear</code>\n<code>/setfilter truck тент</code>\n<code>/setfilter phone on</code>\n<code>/setfilter include flour,rice</code>",
        "setfilter_unknown": "❌ Unknown filter type: {type}\n\nAvailable: weight, truck, phone, include, exclude",
        "filter_weight_usage": "⚖️ Weight filter:\n<code>/setfilter weight 5-25</code> — range\n<code>/setfilter weight min 5</code> — only min\n<code>/setfilter weight max 25</code> — only max\n<code>/setfilter weight clear</code> — clear",
        "filter_phone_usage": "📞 <code>/setfilter phone on</code> or <code>/setfilter phone off</code>",
        "filter_updated": "✅ Filter updated.",
        "filter_cleared": "✅ Filter cleared.",
        "clearfilters_done": "✅ All filters cleared.",
        # Stats
        "stats_user": (
            "📊 <b>Your statistics</b>\n\n"
            "🔎 Searches: <b>{searches}</b>\n"
            "🚛 Cargos received: <b>{cargos}</b>\n"
            "📍 Route: <b>{route}</b>\n"
            "🔔 Notifications: <b>{notify}</b>\n"
            "🌐 Language: <b>{language}</b>"
        ),
        "stats_no_route": "not set",
        # Admin: users
        "admin_users": (
            "👥 <b>User statistics</b>\n\n"
            "Total: <b>{total}</b>\n"
            "Authorized: <b>{authorized}</b>\n"
            "With routes: <b>{with_routes}</b>\n"
            "Active (24h): <b>{active_24h}</b>\n\n"
            "📋 Groups: <b>{groups}</b>"
        ),
        # Broadcast
        "broadcast_usage": "Usage: /broadcast &lt;message text&gt;",
        "broadcast_done": "📢 Sent: {sent} successful, {failed} failed.",
        "broadcast_received": "📢 <b>Announcement</b>\n\n{message}",
        # Account management (admin)
        "accounts_empty": "👥 No accounts.",
        "accounts_header": "👥 <b>Login accounts ({count})</b>\n",
        "accounts_admin_label": "admin",
        "accounts_session_label": "sessions",
        "accounts_routes_label": "routes",
        "accounts_footer": "\nAdd: /adduser &lt;login&gt; &lt;password&gt; [admin]\nDelete: /deluser &lt;login&gt;\nPassword: /resetpass &lt;login&gt; &lt;password&gt;",
        "adduser_usage": "Usage: /adduser &lt;login&gt; &lt;password&gt; [admin]\n\nExample:\n<code>/adduser ali 12345</code>\n<code>/adduser dilshod 12345 admin</code>\n\nLogin: 3-32 chars (a-z, 0-9, _)\nPassword: at least 4 chars",
        "adduser_invalid_login": "❌ Invalid login format: <code>{login}</code>\nOnly latin letters, digits and _, 3-32 chars.",
        "adduser_password_short": "❌ Password too short. Minimum 4 characters.",
        "adduser_already_exists": "❌ Login already exists: <code>{login}</code>",
        "adduser_done": "✅ Account created:\n👤 Login: <code>{login}</code>\n🔑 Password: <code>{password}</code>\n👑 Admin: <b>{admin}</b>\n\n⚠️ <b>Delete this message for security!</b>",
        "deluser_usage": "Usage: /deluser &lt;login&gt;\nExample: /deluser ali",
        "deluser_not_found": "❌ Login not found: <code>{login}</code>",
        "deluser_self": "❌ Cannot delete your own account.",
        "deluser_last_admin": "❌ Cannot delete the last admin. Create another admin first.",
        "deluser_done": "✅ Account deleted: <code>{login}</code>\nActive sessions stopped.",
        "resetpass_usage": "Usage: /resetpass &lt;login&gt; &lt;new password&gt;\nExample: /resetpass ali new12345",
        "resetpass_done": "✅ Password updated: <code>{login}</code>\n🔑 New password: <code>{password}</code>\n\n⚠️ Delete this message for security.",
        "changepass_usage": "Usage: /changepass &lt;new password&gt;\nExample: <code>/changepass new12345</code>",
        "changepass_short": "❌ Password too short. Minimum 4 characters.",
        "changepass_done": "✅ Password changed.\n\n⚠️ Delete this message for security.",
        "makeadmin_usage": "Usage: /makeadmin &lt;login&gt;",
        "unadmin_usage": "Usage: /unadmin &lt;login&gt;",
        "makeadmin_done": "✅ Now admin: <code>{login}</code>",
        "unadmin_done": "✅ Admin removed: <code>{login}</code>",
        "unadmin_last": "❌ Cannot remove the last admin.",
        "unadmin_self": "❌ Cannot remove yourself from admin.",
        # Reply keyboard buttons
        "btn_search": "🔎 Search cargo",
        "btn_route": "📍 Route",
        "btn_filters": "🎯 Filters",
        "btn_stats": "📊 Stats",
        "btn_settings": "⚙️ Settings",
        "btn_help": "ℹ️ Help",
        "btn_admin": "🔧 Admin panel",
        "btn_back": "🔙 Back",
        # Inline buttons / callbacks
        "ibtn_change_route": "📝 Change",
        "ibtn_clear_route": "🗑 Clear",
        "ibtn_notify_on": "🔔 Enable notifications",
        "ibtn_notify_off": "🔕 Disable notifications",
        "ibtn_back": "🔙 Back",
        "ibtn_set_weight": "⚖️ Weight",
        "ibtn_set_truck": "🚛 Truck type",
        "ibtn_set_phone_req": "📞 Phone required",
        "ibtn_set_include": "✅ Required keywords",
        "ibtn_set_exclude": "❌ Excluded keywords",
        "ibtn_clear_filters": "🗑 Clear all",
        "ibtn_change_lang": "🌐 Change language",
        "ibtn_change_pass": "🔑 Change password",
        "ibtn_logout": "🚪 Log out",
        "ibtn_admin_users": "👥 Users",
        "ibtn_admin_groups": "📋 Groups",
        "ibtn_admin_stats": "📊 Stats",
        "ibtn_admin_broadcast": "📢 Broadcast",
        "ibtn_admin_pending": "🆕 New users ({count})",
        "ibtn_add_account": "➕ New account",
        "ibtn_add_group": "➕ Add group",
        "ibtn_make_admin": "👑 Make admin",
        "ibtn_unmake_admin": "👤 Remove admin",
        "ibtn_delete": "🗑 Delete",
        "ibtn_reset_pass": "🔑 Reset password",
        "ibtn_approve": "✅ Approve",
        "ibtn_reject": "🚫 Reject",
        "ibtn_yes": "✅ Yes",
        "ibtn_no": "❌ No",
        "ibtn_cancel": "❌ Cancel",
        # Settings / panels
        "main_menu_hint": "Use the buttons below or send a route (e.g. <i>Bukhara Tashkent</i>):",
        "settings_panel": "⚙️ <b>Settings</b>",
        "admin_panel": "🔧 <b>Admin panel</b>",
        "admin_pending_empty": "🆕 No pending users.",
        "admin_pending_header": "🆕 <b>Unauthorized users ({count})</b>\n\nThey started the bot but haven't logged in. After approval the bot will create their account and send credentials.",
        "admin_pending_item": "👤 <b>{name}</b>\n  ID: <code>{chat_id}</code>\n  Started: {started}",
        "admin_user_actions": "👤 <b>{login}</b>\n  👑 Admin: {is_admin}\n  🔑 Password: <code>{password}</code>\n  💬 Sessions: <b>{sessions}</b>\n  📍 Routes: <b>{routes}</b>",
        "admin_group_actions": "📋 <b>@{username}</b>\n  Title: {title}\n  Added: {added_at}",
        "pending_user_first_seen": "👋 Hello! Authorization is required to use the bot.\n\nYour request was sent to admin. You will receive credentials after approval.",
        "admin_new_pending_notification": "🆕 <b>New user started the bot</b>\n\n👤 {name}\n🆔 <code>{chat_id}</code>\n\n/admin → New users — to approve",
        "credentials_sent_to_user": "✅ Account created and sent to user.\nLogin: <code>{login}</code>",
        "user_received_credentials": "✅ <b>Your account is created!</b>\n\n👤 Login: <code>{login}</code>\n🔑 Password: <code>{password}</code>\n\n⚠️ Remember the password and delete this message for security.\n\nUse /start to log in.",
        "pending_rejected": "🚫 User rejected.",
        "pending_user_was_rejected": "🚫 Your access request was rejected by the administrator.",
        "form_enter_login": "Enter new login (3-32 chars: a-z, 0-9, _):",
        "form_enter_password": "Enter new password (min 4 chars):",
        "form_confirm_admin": "Make this user an admin?",
        "form_enter_route": "Send a new route, for example:\n<code>Bukhara Tashkent</code>",
        "form_enter_truck": "Enter truck type (e.g. тент, реф):\n(or \"clear\" to remove)",
        "form_enter_weight": "Enter weight range:\n<code>5-25</code> — range\n<code>min 5</code> — only min\n<code>max 25</code> — only max\n<code>clear</code> — clear",
        "form_enter_include": "Enter required keywords, comma-separated (e.g. <code>flour,rice</code>):\n(or \"clear\" to remove)",
        "form_enter_exclude": "Enter excluded keywords, comma-separated (e.g. <code>rejected,fine</code>):\n(or \"clear\" to remove)",
        "form_enter_group": "Send group username or link:\n<code>yuk95</code>\n<code>@yuk95</code>\n<code>https://t.me/yuk95</code>",
        "form_enter_broadcast": "Send the broadcast message:",
        "form_enter_new_password": "Send the new password:",
        "form_enter_login_for_reset": "Which login to reset password for?",
        "form_cancelled": "❌ Cancelled.",
        "search_prompt": "🔎 Send a route (e.g. <code>Bukhara Tashkent</code>)",
        # Broadcast targeting
        "bcast_choose_target": "📢 Who to send to?",
        "bcast_to_all": "📢 Everyone",
        "bcast_to_selected": "👤 Selected",
        "bcast_select_users": "👤 Select users ({count} selected):",
        "bcast_no_selected": "❌ No users selected.",
        "bcast_no_users": "❌ No authorized users.",
        "ibtn_bcast_all": "📢 Everyone",
        "ibtn_bcast_select": "👤 Select",
        "ibtn_select_all": "✅ All",
        "ibtn_clear_selection": "🗑 Clear",
        "ibtn_send_to_selected": "📤 Send ({count})",
        # Phone capture
        "phone_request": "📱 Please share your phone number (or skip):",
        "btn_share_phone": "📱 Share phone",
        "btn_skip_phone": "⏭ Skip",
        "phone_received": "✅ Phone saved: {phone}",
        "phone_skipped": "ℹ️ Phone not provided.",
        # Access-request panel (post-phone for unauthorized users)
        "access_no_permission": "ℹ️ You haven't been granted access yet.\n\nIf you have an account from admin, log in with your credentials.\nOtherwise tap <b>📨 Message admin</b> to request access.",
        "btn_request_login": "🔑 I have a login — log in",
        "btn_request_message_admin": "📨 Message admin",
        "waiting_for_admin_reply": "⏳ Your message was sent to admin.\nYou can send another message or log in if you have a login:",
        # Messages module
        "btn_msg_admin": "📨 Message admin",
        "form_enter_message_to_admin": "✍️ Send your message to admin (text, photo or file):",
        "form_enter_reply_to_user": "✍️ Write reply to the user (text, photo or file):",
        "message_sent_to_admin": "✅ Your message has been sent to admin.\nI'll notify you when there's a reply.",
        "message_no_admins": "⚠️ No admins available right now. Try later.",
        "user_received_admin_reply": "📩 <b>Admin reply:</b>",
        "ibtn_admin_messages": "💬 Messages ({count})",
        "ibtn_view_thread": "👁 Open",
        "ibtn_reply_to_user": "✍️ Reply",
        "admin_messages_empty": "💬 No messages.",
        "admin_messages_header": "💬 <b>Messages ({count} threads, {unread} unread)</b>",
        "admin_msg_thread_header": "💬 <b>Thread: {name}</b>",
        "admin_msg_thread_user_info": "👤 <b>{name}</b>\n{username}\n📞 {phone}\n🆔 <code>{chat_id}</code>",
        "admin_new_message_notification": "📨 <b>New message:</b>\n👤 {name}\n📞 {phone}\n\n💬 {preview}",
        "admin_reply_sent": "✅ Reply sent to the user.",
        "msg_user_label": "👤 User",
        "msg_admin_label": "👑 Admin ({login})",
        "unknown_user": "Unknown",
    },
}

def t(key: str, lang: str = DEFAULT_LANGUAGE, **kwargs) -> str:
    table = TRANSLATIONS.get(lang) or TRANSLATIONS[DEFAULT_LANGUAGE]
    template = table.get(key) or TRANSLATIONS[DEFAULT_LANGUAGE].get(key, key)
    return template.format(**kwargs) if kwargs else template

def language_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=label) for label in LANG_BUTTONS.values()]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def parse_language_choice(text: str):
    if not text:
        return None
    t_lower = text.lower()
    if "🇺🇿" in text or "uzbek" in t_lower or "o'zbek" in t_lower or "ozbek" in t_lower or t_lower.strip() == "uz":
        return "uz"
    if "🇷🇺" in text or "русск" in t_lower or t_lower.strip() in ("ru", "ру"):
        return "ru"
    if "🇬🇧" in text or "english" in t_lower or t_lower.strip() == "en":
        return "en"
    return None

# ============================================================
# REPLY / INLINE KEYBOARDS
# ============================================================
MAIN_MENU_BUTTON_KEYS = ("btn_search", "btn_route", "btn_filters",
                        "btn_stats", "btn_settings", "btn_help",
                        "btn_admin", "btn_back", "btn_msg_admin")

def main_menu_keyboard(lang: str, is_admin: bool) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text=t("btn_search", lang))],
        [KeyboardButton(text=t("btn_route", lang)), KeyboardButton(text=t("btn_filters", lang))],
        [KeyboardButton(text=t("btn_stats", lang)), KeyboardButton(text=t("btn_settings", lang))],
    ]
    if is_admin:
        rows.append([KeyboardButton(text=t("btn_admin", lang)), KeyboardButton(text=t("btn_help", lang))])
    else:
        rows.append([KeyboardButton(text=t("btn_msg_admin", lang)), KeyboardButton(text=t("btn_help", lang))])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, is_persistent=True)

def phone_request_keyboard(lang: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t("btn_share_phone", lang), request_contact=True)],
            [KeyboardButton(text=t("btn_skip_phone", lang))],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def access_request_keyboard(lang: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t("btn_request_message_admin", lang))],
            [KeyboardButton(text=t("btn_request_login", lang))],
        ],
        resize_keyboard=True,
    )

def is_access_button_login(text: str) -> bool:
    if not text:
        return False
    for lang in TRANSLATIONS:
        if TRANSLATIONS[lang].get("btn_request_login") == text:
            return True
    return False

def is_access_button_msg_admin(text: str) -> bool:
    if not text:
        return False
    for lang in TRANSLATIONS:
        if TRANSLATIONS[lang].get("btn_request_message_admin") == text:
            return True
    return False

def detect_main_button(text: str):
    """Return action key (e.g. 'search'/'route'/...) if text matches a main-menu button in any language, else None."""
    if not text:
        return None
    for lang in TRANSLATIONS:
        for key in MAIN_MENU_BUTTON_KEYS:
            if TRANSLATIONS[lang].get(key) == text:
                return key.replace("btn_", "")
    return None

def is_skip_phone_text(text: str) -> bool:
    if not text:
        return False
    for lang in TRANSLATIONS:
        if TRANSLATIONS[lang].get("btn_skip_phone") == text:
            return True
    return text.strip().lower() in ("skip", "пропустить", "o'tkazib", "otkazib")

def route_panel_keyboard(lang: str, notify_on: bool) -> InlineKeyboardMarkup:
    """Legacy single-route panel (kept for compatibility)."""
    notify_btn = ("ibtn_notify_off" if notify_on else "ibtn_notify_on")
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=t("ibtn_change_route", lang), callback_data="cb:route_change"),
            InlineKeyboardButton(text=t("ibtn_clear_route", lang), callback_data="cb:route_clear"),
        ],
        [InlineKeyboardButton(text=t(notify_btn, lang), callback_data="cb:route_notify_toggle")],
    ])

def routes_panel_keyboard(lang: str, routes: list, notify_on: bool) -> InlineKeyboardMarkup:
    rows = []
    for r in routes:
        label = f"🗑 {r['from_city']} ➜ {r['to_city']}"
        rows.append([InlineKeyboardButton(
            text=label[:60],
            callback_data=f"cb:autoroute_del:{r['id']}",
        )])
    rows.append([InlineKeyboardButton(
        text=t("ibtn_add_route", lang),
        callback_data="cb:autoroute_add",
    )])
    notify_btn = "ibtn_notify_off" if notify_on else "ibtn_notify_on"
    rows.append([InlineKeyboardButton(
        text=t(notify_btn, lang),
        callback_data="cb:route_notify_toggle",
    )])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def filters_panel_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=t("ibtn_set_weight", lang), callback_data="cb:filter_weight"),
            InlineKeyboardButton(text=t("ibtn_set_truck", lang), callback_data="cb:filter_truck"),
        ],
        [InlineKeyboardButton(text=t("ibtn_set_phone_req", lang), callback_data="cb:filter_phone")],
        [
            InlineKeyboardButton(text=t("ibtn_set_include", lang), callback_data="cb:filter_include"),
            InlineKeyboardButton(text=t("ibtn_set_exclude", lang), callback_data="cb:filter_exclude"),
        ],
        [InlineKeyboardButton(text=t("ibtn_clear_filters", lang), callback_data="cb:filter_clear_all")],
    ])

def settings_panel_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("ibtn_change_lang", lang), callback_data="cb:set_lang")],
        [InlineKeyboardButton(text=t("ibtn_change_pass", lang), callback_data="cb:change_pass")],
        [InlineKeyboardButton(text=t("ibtn_logout", lang), callback_data="cb:logout")],
    ])

def admin_panel_keyboard(lang: str, pending_count: int, unread_msgs: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text=t("ibtn_admin_users", lang), callback_data="cb:admin_users"),
            InlineKeyboardButton(text=t("ibtn_admin_groups", lang), callback_data="cb:admin_groups"),
        ],
        [
            InlineKeyboardButton(text=t("ibtn_admin_stats", lang), callback_data="cb:admin_stats"),
            InlineKeyboardButton(text=t("ibtn_admin_broadcast", lang), callback_data="cb:admin_broadcast"),
        ],
        [
            InlineKeyboardButton(
                text=t("ibtn_admin_pending", lang, count=pending_count),
                callback_data="cb:admin_pending",
            ),
        ],
        [
            InlineKeyboardButton(
                text=t("ibtn_admin_messages", lang, count=unread_msgs),
                callback_data="cb:admin_messages",
            ),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_messages_keyboard(lang: str, conversations: list) -> InlineKeyboardMarkup:
    rows = []
    for conv in conversations:
        unread_badge = f" [{conv['unread']}]" if conv["unread"] else ""
        # We don't have a name yet in this query — caller fills it in.
        label = f"{conv.get('name', '?')}{unread_badge} — {conv['preview'] or '...'}"
        rows.append([InlineKeyboardButton(
            text=label[:60],
            callback_data=f"cb:msg_open:{conv['user_chat_id']}",
        )])
    rows.append([InlineKeyboardButton(text=t("ibtn_back", lang), callback_data="cb:admin_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_msg_thread_keyboard(lang: str, user_chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("ibtn_reply_to_user", lang),
                              callback_data=f"cb:msg_reply:{user_chat_id}")],
        [InlineKeyboardButton(text=t("ibtn_back", lang),
                              callback_data="cb:admin_messages")],
    ])

def admin_users_keyboard(lang: str, accounts: list, own_login: str) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=t("ibtn_add_account", lang), callback_data="cb:add_user")]]
    for acc in accounts:
        badge = " 👑" if acc["is_admin"] else ""
        rows.append([InlineKeyboardButton(
            text=f"{acc['login']}{badge}",
            callback_data=f"cb:user:{acc['login']}",
        )])
    rows.append([InlineKeyboardButton(text=t("ibtn_back", lang), callback_data="cb:admin_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_user_actions_keyboard(lang: str, login: str, is_admin: bool, is_self: bool) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=t("ibtn_reset_pass", lang), callback_data=f"cb:user_reset:{login}")]]
    if not is_self:
        if is_admin:
            rows.append([InlineKeyboardButton(text=t("ibtn_unmake_admin", lang), callback_data=f"cb:user_unadmin:{login}")])
        else:
            rows.append([InlineKeyboardButton(text=t("ibtn_make_admin", lang), callback_data=f"cb:user_makeadmin:{login}")])
        rows.append([InlineKeyboardButton(text=t("ibtn_delete", lang), callback_data=f"cb:user_delete:{login}")])
    rows.append([InlineKeyboardButton(text=t("ibtn_back", lang), callback_data="cb:admin_users")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_groups_keyboard(lang: str, groups: list) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=t("ibtn_add_group", lang), callback_data="cb:add_group")]]
    for username, title in groups[:30]:
        display = (title or username)[:30]
        rows.append([InlineKeyboardButton(
            text=f"@{username} — {display}",
            callback_data=f"cb:group:{username}",
        )])
    rows.append([InlineKeyboardButton(text=t("ibtn_back", lang), callback_data="cb:admin_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_group_actions_keyboard(lang: str, username: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("ibtn_delete", lang), callback_data=f"cb:group_delete:{username}")],
        [InlineKeyboardButton(text=t("ibtn_back", lang), callback_data="cb:admin_groups")],
    ])

def admin_pending_keyboard(lang: str, pending: list) -> InlineKeyboardMarkup:
    rows = []
    for p in pending:
        rows.append([InlineKeyboardButton(
            text=f"{p['name']} (id {p['chat_id']})",
            callback_data=f"cb:pending:{p['chat_id']}",
        )])
    rows.append([InlineKeyboardButton(text=t("ibtn_back", lang), callback_data="cb:admin_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_pending_actions_keyboard(lang: str, chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=t("ibtn_approve", lang), callback_data=f"cb:approve:{chat_id}"),
            InlineKeyboardButton(text=t("ibtn_reject", lang), callback_data=f"cb:reject:{chat_id}"),
        ],
        [InlineKeyboardButton(text=t("ibtn_back", lang), callback_data="cb:admin_pending")],
    ])

def yes_no_keyboard(lang: str, yes_data: str, no_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=t("ibtn_yes", lang), callback_data=yes_data),
        InlineKeyboardButton(text=t("ibtn_no", lang), callback_data=no_data),
    ]])

def cancel_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=t("ibtn_cancel", lang), callback_data="cb:cancel"),
    ]])

def broadcast_target_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=t("ibtn_bcast_all", lang), callback_data="cb:bcast_all"),
            InlineKeyboardButton(text=t("ibtn_bcast_select", lang), callback_data="cb:bcast_select"),
        ],
        [InlineKeyboardButton(text=t("ibtn_back", lang), callback_data="cb:admin_back")],
    ])

def broadcast_users_keyboard(lang: str, accounts: list, selected_logins: set) -> InlineKeyboardMarkup:
    rows = []
    # Per-user toggle buttons (only logins that have at least 1 active session)
    eligible = [a for a in accounts if a["sessions"] > 0]
    for acc in eligible:
        prefix = "☑" if acc["login"] in selected_logins else "☐"
        rows.append([InlineKeyboardButton(
            text=f"{prefix} {acc['login']} ({acc['sessions']})",
            callback_data=f"cb:bcast_toggle:{acc['login']}",
        )])
    if not rows:
        # No eligible users — only show back button
        rows.append([InlineKeyboardButton(text=t("ibtn_back", lang), callback_data="cb:admin_back")])
        return InlineKeyboardMarkup(inline_keyboard=rows)
    rows.append([
        InlineKeyboardButton(text=t("ibtn_select_all", lang), callback_data="cb:bcast_pick_all"),
        InlineKeyboardButton(text=t("ibtn_clear_selection", lang), callback_data="cb:bcast_clear_sel"),
    ])
    rows.append([InlineKeyboardButton(
        text=t("ibtn_send_to_selected", lang, count=len(selected_logins)),
        callback_data="cb:bcast_send_selected",
    )])
    rows.append([InlineKeyboardButton(text=t("ibtn_back", lang), callback_data="cb:admin_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def db_chat_ids_by_logins(logins):
    """Resolve a set of logins to all their authorized chat_ids."""
    if not logins:
        return []
    placeholders = ",".join("?" * len(logins))
    with db() as conn:
        rows = conn.execute(
            f"SELECT chat_id FROM users WHERE login IN ({placeholders}) AND is_authorized = 1",
            list(logins),
        ).fetchall()
    return [r["chat_id"] for r in rows]

# ============================================================
# PASSWORD HASHING
# ============================================================
def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    iterations = 200_000
    key = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${key.hex()}"

def verify_password(password: str, stored: str) -> bool:
    try:
        algo, iters_s, salt_hex, key_hex = stored.split("$")
        if algo != "pbkdf2_sha256":
            return False
        iterations = int(iters_s)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(key_hex)
        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
        return secrets.compare_digest(actual, expected)
    except Exception:
        log.exception("Password verification error")
        return False

# ============================================================
# DATABASE
# ============================================================
@contextmanager
def db():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def _table_columns(conn, table: str):
    return [row["name"] for row in conn.execute(f"PRAGMA table_info({table})")]

def init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS credentials (
            login TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            last_known_password TEXT
        );
        CREATE TABLE IF NOT EXISTS users (
            chat_id INTEGER PRIMARY KEY,
            login TEXT,
            is_authorized INTEGER NOT NULL DEFAULT 0,
            language TEXT NOT NULL DEFAULT 'ru',
            from_city TEXT,
            to_city TEXT,
            total_searches INTEGER NOT NULL DEFAULT 0,
            total_cargos_received INTEGER NOT NULL DEFAULT 0,
            phone TEXT,
            first_name TEXT,
            telegram_username TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS sent_cargos (
            chat_id INTEGER NOT NULL,
            cargo_hash TEXT NOT NULL,
            sent_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (chat_id, cargo_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_sent_cargos_sent_at ON sent_cargos(sent_at);
        CREATE TABLE IF NOT EXISTS groups (
            username TEXT PRIMARY KEY,
            title TEXT,
            added_by INTEGER,
            added_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS user_filters (
            chat_id INTEGER PRIMARY KEY,
            weight_min REAL,
            weight_max REAL,
            truck_type TEXT,
            require_phone INTEGER NOT NULL DEFAULT 0,
            keywords_include TEXT NOT NULL DEFAULT '',
            keywords_exclude TEXT NOT NULL DEFAULT '',
            notifications_enabled INTEGER NOT NULL DEFAULT 1,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_chat_id INTEGER NOT NULL,
            direction TEXT NOT NULL,
            sender_chat_id INTEGER,
            sender_login TEXT,
            text TEXT,
            file_id TEXT,
            file_type TEXT,
            is_read INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_chat_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_messages_unread ON messages(direction, is_read);
        CREATE TABLE IF NOT EXISTS auto_routes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            from_city TEXT NOT NULL,
            to_city TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_auto_routes_chat ON auto_routes(chat_id, enabled);
        CREATE TABLE IF NOT EXISTS cargo_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_username TEXT NOT NULL,
            group_title TEXT,
            message_id INTEGER,
            text TEXT NOT NULL,
            posted_at TIMESTAMP NOT NULL,
            sender_id INTEGER,
            sender_name TEXT,
            sender_username TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(group_username, message_id)
        );
        CREATE INDEX IF NOT EXISTS idx_cargo_messages_recent
            ON cargo_messages(posted_at DESC);
        CREATE INDEX IF NOT EXISTS idx_cargo_messages_group
            ON cargo_messages(group_username, posted_at DESC);
        """)

        # Migrations for older DBs
        cred_cols = _table_columns(conn, "credentials")
        if "is_admin" not in cred_cols:
            conn.execute("ALTER TABLE credentials ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")
            conn.execute("UPDATE credentials SET is_admin = 1 WHERE login = 'admin'")
        if "last_known_password" not in cred_cols:
            conn.execute("ALTER TABLE credentials ADD COLUMN last_known_password TEXT")

        user_cols = _table_columns(conn, "users")
        if "language" not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN language TEXT NOT NULL DEFAULT 'ru'")
        if "total_searches" not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN total_searches INTEGER NOT NULL DEFAULT 0")
        if "total_cargos_received" not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN total_cargos_received INTEGER NOT NULL DEFAULT 0")
        if "phone" not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN phone TEXT")
        if "first_name" not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN first_name TEXT")
        if "telegram_username" not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN telegram_username TEXT")

        # Migrate existing single-route data into the new auto_routes table (idempotent)
        existing = conn.execute("""
            SELECT chat_id, from_city, to_city FROM users
            WHERE from_city IS NOT NULL AND to_city IS NOT NULL
        """).fetchall()
        migrated = 0
        for r in existing:
            dup = conn.execute("""
                SELECT 1 FROM auto_routes
                WHERE chat_id = ? AND lower(from_city) = lower(?) AND lower(to_city) = lower(?)
            """, (r["chat_id"], r["from_city"], r["to_city"])).fetchone()
            if not dup:
                conn.execute(
                    "INSERT INTO auto_routes (chat_id, from_city, to_city) VALUES (?, ?, ?)",
                    (r["chat_id"], r["from_city"], r["to_city"]),
                )
                migrated += 1
        if migrated:
            log.info("Migrated %d single-routes into auto_routes", migrated)

        # Seed credentials
        if conn.execute("SELECT COUNT(*) AS c FROM credentials").fetchone()["c"] == 0:
            seed_creds = [
                ("admin", INITIAL_ADMIN_PASSWORD, 1),
                ("logistics", INITIAL_LOGISTICS_PASSWORD, 0),
            ]
            for login, password, is_admin in seed_creds:
                conn.execute(
                    "INSERT INTO credentials (login, password_hash, is_admin, last_known_password) VALUES (?, ?, ?, ?)",
                    (login, hash_password(password), is_admin, password),
                )
            log.info("Seeded credentials: %s", [c[0] for c in seed_creds])

        # Seed groups
        if conn.execute("SELECT COUNT(*) AS c FROM groups").fetchone()["c"] == 0:
            for username, title in DEFAULT_GROUPS:
                conn.execute(
                    "INSERT INTO groups (username, title) VALUES (?, ?)",
                    (username, title),
                )
            log.info("Seeded %d default groups", len(DEFAULT_GROUPS))

# ---------- credentials / auth ----------
def db_check_credentials(login: str, password: str):
    with db() as conn:
        row = conn.execute(
            "SELECT password_hash, is_admin FROM credentials WHERE login = ?",
            (login,),
        ).fetchone()
    if not row:
        return False, False
    if not verify_password(password, row["password_hash"]):
        return False, False
    return True, bool(row["is_admin"])

def db_set_authorized(chat_id: int, login: str, language: str = DEFAULT_LANGUAGE):
    with db() as conn:
        conn.execute("""
            INSERT INTO users (chat_id, login, is_authorized, language) VALUES (?, ?, 1, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                login = excluded.login,
                is_authorized = 1,
                updated_at = CURRENT_TIMESTAMP
        """, (chat_id, login, language))
    _invalidate_routes_cache_safe()

def db_set_unauthorized(chat_id: int):
    with db() as conn:
        conn.execute(
            "UPDATE users SET is_authorized = 0, updated_at = CURRENT_TIMESTAMP WHERE chat_id = ?",
            (chat_id,),
        )
    _invalidate_routes_cache_safe()

def _invalidate_routes_cache_safe():
    """Best-effort invalidation: defined later in the file may not yet be available during init."""
    global _active_routes_cache
    _active_routes_cache = None

def db_is_authorized(chat_id: int) -> bool:
    with db() as conn:
        row = conn.execute(
            "SELECT is_authorized FROM users WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
    return bool(row) and bool(row["is_authorized"])

def db_is_admin(chat_id: int) -> bool:
    with db() as conn:
        row = conn.execute("""
            SELECT c.is_admin
            FROM users u JOIN credentials c ON c.login = u.login
            WHERE u.chat_id = ? AND u.is_authorized = 1
        """, (chat_id,)).fetchone()
    return bool(row) and bool(row["is_admin"])

def db_get_user_login(chat_id: int):
    with db() as conn:
        row = conn.execute(
            "SELECT login FROM users WHERE chat_id = ? AND is_authorized = 1",
            (chat_id,),
        ).fetchone()
    return row["login"] if row else None

# ---------- account management ----------
def db_login_exists(login: str) -> bool:
    with db() as conn:
        row = conn.execute(
            "SELECT 1 FROM credentials WHERE login = ?", (login,)
        ).fetchone()
    return row is not None

def db_count_admins() -> int:
    with db() as conn:
        return conn.execute(
            "SELECT COUNT(*) AS c FROM credentials WHERE is_admin = 1"
        ).fetchone()["c"]

def db_create_account(login: str, password: str, is_admin: bool) -> bool:
    """Create new credentials entry. Returns False if login already exists."""
    if db_login_exists(login):
        return False
    with db() as conn:
        conn.execute(
            "INSERT INTO credentials (login, password_hash, is_admin, last_known_password) VALUES (?, ?, ?, ?)",
            (login, hash_password(password), 1 if is_admin else 0, password),
        )
    return True

def db_delete_account(login: str) -> str:
    """Delete account. Returns 'ok' / 'not_found' / 'last_admin'."""
    with db() as conn:
        row = conn.execute(
            "SELECT is_admin FROM credentials WHERE login = ?", (login,)
        ).fetchone()
        if not row:
            return "not_found"
        if row["is_admin"]:
            admin_count = conn.execute(
                "SELECT COUNT(*) AS c FROM credentials WHERE is_admin = 1"
            ).fetchone()["c"]
            if admin_count <= 1:
                return "last_admin"
        # End all sessions for this login
        conn.execute(
            "UPDATE users SET is_authorized = 0, updated_at = CURRENT_TIMESTAMP WHERE login = ?",
            (login,),
        )
        conn.execute("DELETE FROM credentials WHERE login = ?", (login,))
    return "ok"

def db_set_password(login: str, new_password: str, store_plaintext: bool = False) -> bool:
    """Set new password. If store_plaintext=True, the new password is also kept in
    last_known_password (used by admin password resets so admin can read it back).
    For user-initiated /changepass we clear last_known_password."""
    if not db_login_exists(login):
        return False
    with db() as conn:
        if store_plaintext:
            conn.execute(
                "UPDATE credentials SET password_hash = ?, last_known_password = ? WHERE login = ?",
                (hash_password(new_password), new_password, login),
            )
        else:
            conn.execute(
                "UPDATE credentials SET password_hash = ?, last_known_password = NULL WHERE login = ?",
                (hash_password(new_password), login),
            )
    return True

def db_get_last_password(login: str):
    with db() as conn:
        row = conn.execute(
            "SELECT last_known_password FROM credentials WHERE login = ?",
            (login,),
        ).fetchone()
    return row["last_known_password"] if row else None

def db_set_admin_flag(login: str, is_admin: bool) -> str:
    """Promote/demote. Returns 'ok' / 'not_found' / 'last_admin'."""
    with db() as conn:
        row = conn.execute(
            "SELECT is_admin FROM credentials WHERE login = ?", (login,)
        ).fetchone()
        if not row:
            return "not_found"
        if row["is_admin"] and not is_admin:
            admin_count = conn.execute(
                "SELECT COUNT(*) AS c FROM credentials WHERE is_admin = 1"
            ).fetchone()["c"]
            if admin_count <= 1:
                return "last_admin"
        conn.execute(
            "UPDATE credentials SET is_admin = ? WHERE login = ?",
            (1 if is_admin else 0, login),
        )
    return "ok"

def db_record_pending(chat_id: int, name: str, language: str = DEFAULT_LANGUAGE):
    """Record a chat that started the bot but isn't authorized."""
    with db() as conn:
        conn.execute("""
            INSERT INTO users (chat_id, login, language) VALUES (?, NULL, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                language = COALESCE(users.language, excluded.language),
                updated_at = CURRENT_TIMESTAMP
        """, (chat_id, language))
        conn.execute(
            "UPDATE users SET login = COALESCE(login, NULL) WHERE chat_id = ?",
            (chat_id,),
        )
        # Store the display name in login field temporarily? No - use a separate column would be cleaner,
        # but for simplicity we keep name in memory only for the notification.

def db_list_pending():
    """Users who started the bot but never authorized (login IS NULL)."""
    with db() as conn:
        rows = conn.execute("""
            SELECT chat_id, language, updated_at FROM users
            WHERE is_authorized = 0 AND login IS NULL
            ORDER BY updated_at DESC LIMIT 50
        """).fetchall()
    return [{"chat_id": r["chat_id"], "language": r["language"],
             "started": r["updated_at"]} for r in rows]

def db_count_pending() -> int:
    with db() as conn:
        return conn.execute(
            "SELECT COUNT(*) AS c FROM users WHERE is_authorized = 0 AND login IS NULL"
        ).fetchone()["c"]

def db_remove_pending(chat_id: int):
    """Used when admin rejects: drop the pending row entirely."""
    with db() as conn:
        conn.execute(
            "DELETE FROM users WHERE chat_id = ? AND is_authorized = 0 AND login IS NULL",
            (chat_id,),
        )

def db_link_login_to_chat(chat_id: int, login: str):
    """Associate a (newly created) login with the user's chat row without authorizing yet.
    Used after admin approves a pending user — the row stays so language/phone are preserved,
    but `login IS NOT NULL` removes them from the pending list."""
    with db() as conn:
        conn.execute("""
            INSERT INTO users (chat_id, login) VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                login = excluded.login,
                updated_at = CURRENT_TIMESTAMP
        """, (chat_id, login))

def db_admin_chat_ids():
    """All chat IDs of currently authorized admins (for notifications)."""
    with db() as conn:
        rows = conn.execute("""
            SELECT u.chat_id FROM users u JOIN credentials c ON c.login = u.login
            WHERE u.is_authorized = 1 AND c.is_admin = 1
        """).fetchall()
    return [r["chat_id"] for r in rows]

# ---------- phone & profile ----------
def db_set_phone(chat_id: int, phone: str):
    if not phone:
        return
    cleaned = re.sub(r"[^\d+]", "", phone)[:20]
    with db() as conn:
        conn.execute(
            "UPDATE users SET phone = ?, updated_at = CURRENT_TIMESTAMP WHERE chat_id = ?",
            (cleaned, chat_id),
        )

def db_get_phone(chat_id: int):
    with db() as conn:
        row = conn.execute("SELECT phone FROM users WHERE chat_id = ?", (chat_id,)).fetchone()
    return row["phone"] if row and row["phone"] else None

def db_set_profile(chat_id: int, first_name: str = None, telegram_username: str = None):
    with db() as conn:
        if first_name is not None:
            conn.execute(
                "UPDATE users SET first_name = ? WHERE chat_id = ?",
                (first_name[:100], chat_id),
            )
        if telegram_username is not None:
            conn.execute(
                "UPDATE users SET telegram_username = ? WHERE chat_id = ?",
                (telegram_username[:100], chat_id),
            )

def db_get_profile(chat_id: int):
    with db() as conn:
        row = conn.execute(
            "SELECT phone, first_name, telegram_username FROM users WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
    if not row:
        return {"phone": None, "first_name": None, "telegram_username": None}
    return {
        "phone": row["phone"],
        "first_name": row["first_name"],
        "telegram_username": row["telegram_username"],
    }

# ---------- messages ----------
def db_save_message(user_chat_id: int, direction: str, sender_chat_id: int,
                    sender_login: str, text: str, file_id: str = None, file_type: str = None) -> int:
    with db() as conn:
        cur = conn.execute("""
            INSERT INTO messages
            (user_chat_id, direction, sender_chat_id, sender_login, text, file_id, file_type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (user_chat_id, direction, sender_chat_id, sender_login, text, file_id, file_type))
        return cur.lastrowid

def db_count_unread_admin() -> int:
    with db() as conn:
        return conn.execute(
            "SELECT COUNT(*) AS c FROM messages WHERE direction = 'in' AND is_read = 0"
        ).fetchone()["c"]

def db_list_conversations(limit: int = 30):
    """Distinct user_chat_ids with their latest message and unread count."""
    with db() as conn:
        rows = conn.execute(f"""
            SELECT m.user_chat_id,
                   MAX(m.created_at) AS latest_at,
                   SUM(CASE WHEN m.direction = 'in' AND m.is_read = 0 THEN 1 ELSE 0 END) AS unread
            FROM messages m
            GROUP BY m.user_chat_id
            ORDER BY MAX(m.created_at) DESC
            LIMIT ?
        """, (limit,)).fetchall()
        out = []
        for r in rows:
            last = conn.execute("""
                SELECT text, file_type FROM messages
                WHERE user_chat_id = ?
                ORDER BY created_at DESC LIMIT 1
            """, (r["user_chat_id"],)).fetchone()
            preview = ""
            if last:
                if last["text"]:
                    preview = last["text"][:40]
                elif last["file_type"]:
                    preview = f"[{last['file_type']}]"
            out.append({
                "user_chat_id": r["user_chat_id"],
                "latest_at": r["latest_at"],
                "unread": int(r["unread"] or 0),
                "preview": preview,
            })
    return out

def db_get_conversation(user_chat_id: int, limit: int = 30):
    with db() as conn:
        rows = conn.execute("""
            SELECT id, direction, sender_login, text, file_id, file_type, created_at
            FROM messages WHERE user_chat_id = ?
            ORDER BY created_at ASC LIMIT ?
        """, (user_chat_id, limit)).fetchall()
    return [dict(r) for r in rows]

def db_mark_conversation_read(user_chat_id: int):
    with db() as conn:
        conn.execute("""
            UPDATE messages SET is_read = 1
            WHERE user_chat_id = ? AND direction = 'in'
        """, (user_chat_id,))

def generate_login(base_hint: str = "") -> str:
    """Generate a unique login. Tries base_hint first, then random suffix."""
    base = re.sub(r"[^a-zA-Z0-9_]", "", base_hint.lower())[:20] if base_hint else ""
    if len(base) < 3:
        base = "user"
    candidate = base
    if not db_login_exists(candidate):
        return candidate
    for _ in range(20):
        suffix = secrets.token_hex(2)
        candidate = f"{base}_{suffix}"[:32]
        if not db_login_exists(candidate):
            return candidate
    return f"user_{secrets.token_hex(4)}"

def generate_password() -> str:
    """8-character password with letters and digits."""
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"
    return "".join(secrets.choice(alphabet) for _ in range(8))

def db_list_accounts():
    """Returns list of dicts: login, is_admin, sessions, routes."""
    with db() as conn:
        rows = conn.execute("""
            SELECT c.login, c.is_admin,
                   COALESCE(SUM(CASE WHEN u.is_authorized = 1 THEN 1 ELSE 0 END), 0) AS sessions,
                   COALESCE(SUM(CASE WHEN u.from_city IS NOT NULL AND u.is_authorized = 1 THEN 1 ELSE 0 END), 0) AS routes
            FROM credentials c
            LEFT JOIN users u ON u.login = c.login
            GROUP BY c.login, c.is_admin
            ORDER BY c.is_admin DESC, c.login ASC
        """).fetchall()
    return [
        {"login": r["login"], "is_admin": bool(r["is_admin"]),
         "sessions": int(r["sessions"]), "routes": int(r["routes"])}
        for r in rows
    ]

# ---------- language ----------
def db_get_language(chat_id: int) -> str:
    with db() as conn:
        row = conn.execute(
            "SELECT language FROM users WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
    if row and row["language"] in TRANSLATIONS:
        return row["language"]
    return DEFAULT_LANGUAGE

def db_set_language(chat_id: int, language: str):
    if language not in TRANSLATIONS:
        return
    with db() as conn:
        conn.execute("""
            INSERT INTO users (chat_id, language) VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                language = excluded.language,
                updated_at = CURRENT_TIMESTAMP
        """, (chat_id, language))

# ---------- routes ----------
def db_set_route(chat_id: int, from_city: str, to_city: str):
    with db() as conn:
        conn.execute("""
            INSERT INTO users (chat_id, from_city, to_city) VALUES (?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                from_city = excluded.from_city,
                to_city = excluded.to_city,
                updated_at = CURRENT_TIMESTAMP
        """, (chat_id, from_city, to_city))
    _invalidate_routes_cache_safe()

def db_clear_route(chat_id: int):
    with db() as conn:
        conn.execute("""
            UPDATE users SET from_city = NULL, to_city = NULL,
                             updated_at = CURRENT_TIMESTAMP
            WHERE chat_id = ?
        """, (chat_id,))
    _invalidate_routes_cache_safe()

def db_get_route(chat_id: int):
    with db() as conn:
        row = conn.execute(
            "SELECT from_city, to_city FROM users WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
    if not row or not row["from_city"] or not row["to_city"]:
        return None
    return (row["from_city"], row["to_city"])

def db_get_active_routes_with_filters():
    """Returns list of (chat_id, from_city, to_city, language, filters_dict) for every
    enabled auto-route belonging to an authorized user with notifications on."""
    with db() as conn:
        rows = conn.execute("""
            SELECT a.chat_id, a.from_city, a.to_city, u.language,
                   f.weight_min, f.weight_max, f.truck_type, f.require_phone,
                   f.keywords_include, f.keywords_exclude, f.notifications_enabled
            FROM auto_routes a
            JOIN users u ON u.chat_id = a.chat_id
            LEFT JOIN user_filters f ON f.chat_id = a.chat_id
            WHERE a.enabled = 1 AND u.is_authorized = 1
        """).fetchall()
    out = []
    for r in rows:
        notifications = r["notifications_enabled"] if r["notifications_enabled"] is not None else 1
        if not notifications:
            continue
        filters = {
            "weight_min": r["weight_min"],
            "weight_max": r["weight_max"],
            "truck_type": r["truck_type"],
            "require_phone": int(r["require_phone"] or 0),
            "keywords_include": r["keywords_include"] or "",
            "keywords_exclude": r["keywords_exclude"] or "",
        }
        out.append((r["chat_id"], r["from_city"], r["to_city"], r["language"], filters))
    return out

# ---------- auto_routes (subscriptions) ----------
def db_add_auto_route(chat_id: int, from_city: str, to_city: str) -> bool:
    """Returns True if created, False if duplicate."""
    with db() as conn:
        existing = conn.execute("""
            SELECT id FROM auto_routes
            WHERE chat_id = ? AND lower(from_city) = lower(?) AND lower(to_city) = lower(?)
        """, (chat_id, from_city, to_city)).fetchone()
        if existing:
            return False
        conn.execute(
            "INSERT INTO auto_routes (chat_id, from_city, to_city) VALUES (?, ?, ?)",
            (chat_id, from_city, to_city),
        )
    _invalidate_routes_cache_safe()
    return True

def db_list_auto_routes(chat_id: int):
    with db() as conn:
        rows = conn.execute("""
            SELECT id, from_city, to_city, enabled, created_at
            FROM auto_routes
            WHERE chat_id = ?
            ORDER BY created_at DESC
        """, (chat_id,)).fetchall()
    return [dict(r) for r in rows]

def db_delete_auto_route(route_id: int, chat_id: int) -> bool:
    """Delete by id, only if it belongs to chat_id."""
    with db() as conn:
        cur = conn.execute(
            "DELETE FROM auto_routes WHERE id = ? AND chat_id = ?",
            (route_id, chat_id),
        )
    _invalidate_routes_cache_safe()
    return cur.rowcount > 0

def db_count_auto_routes(chat_id: int) -> int:
    with db() as conn:
        return conn.execute(
            "SELECT COUNT(*) AS c FROM auto_routes WHERE chat_id = ? AND enabled = 1",
            (chat_id,),
        ).fetchone()["c"]

# ---------- sent cargo dedupe ----------
def db_already_sent(chat_id: int, cargo_hash_value: str) -> bool:
    with db() as conn:
        row = conn.execute(
            "SELECT 1 FROM sent_cargos WHERE chat_id = ? AND cargo_hash = ?",
            (chat_id, cargo_hash_value),
        ).fetchone()
    return row is not None

def db_mark_sent(chat_id: int, cargo_hash_value: str):
    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO sent_cargos (chat_id, cargo_hash) VALUES (?, ?)",
            (chat_id, cargo_hash_value),
        )

def db_cleanup_sent():
    cutoff = (datetime.now(UZ_TIME) - timedelta(hours=SENT_CARGO_TTL_HOURS)) \
        .strftime("%Y-%m-%d %H:%M:%S")
    with db() as conn:
        cur = conn.execute("DELETE FROM sent_cargos WHERE sent_at < ?", (cutoff,))
        if cur.rowcount > 0:
            log.info("Cleaned %d expired sent_cargos", cur.rowcount)

# ---------- local cargo message store (bot-only mode) ----------
def db_store_cargo_message(group_username: str, group_title: str,
                           message_id: int, text: str, posted_at,
                           sender_id=None, sender_name=None, sender_username=None):
    """Persist a group message so search can scan it later. Idempotent on
    (group_username, message_id)."""
    posted_str = posted_at.strftime("%Y-%m-%d %H:%M:%S") \
        if hasattr(posted_at, "strftime") else str(posted_at)
    with db() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO cargo_messages
            (group_username, group_title, message_id, text, posted_at,
             sender_id, sender_name, sender_username)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (group_username, group_title, message_id, text, posted_str,
              sender_id, sender_name, sender_username))

def db_iter_recent_messages(cutoff_dt):
    """Yield messages newer than cutoff_dt across all monitored groups,
    newest first."""
    cutoff_str = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")
    with db() as conn:
        rows = conn.execute("""
            SELECT group_username, group_title, message_id, text, posted_at,
                   sender_id, sender_name, sender_username
            FROM cargo_messages
            WHERE posted_at >= ?
            ORDER BY posted_at DESC
        """, (cutoff_str,)).fetchall()
    return rows

def db_cleanup_cargo_messages(retention_hours: int = 48):
    """Remove cargo messages older than retention_hours."""
    cutoff = (datetime.now(UZ_TIME) - timedelta(hours=retention_hours)) \
        .strftime("%Y-%m-%d %H:%M:%S")
    with db() as conn:
        cur = conn.execute("DELETE FROM cargo_messages WHERE posted_at < ?", (cutoff,))
        if cur.rowcount > 0:
            log.info("Cleaned %d old cargo_messages", cur.rowcount)

# ---------- groups ----------
def db_list_groups():
    with db() as conn:
        rows = conn.execute("SELECT username, title FROM groups ORDER BY username").fetchall()
    return [(r["username"], r["title"]) for r in rows]

def db_get_group_usernames():
    with db() as conn:
        rows = conn.execute("SELECT username FROM groups").fetchall()
    return {r["username"] for r in rows}

def db_get_group_title(username: str):
    with db() as conn:
        row = conn.execute("SELECT title FROM groups WHERE username = ?", (username,)).fetchone()
    return row["title"] if row else None

def db_add_group(username: str, title: str, added_by: int) -> bool:
    with db() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO groups (username, title, added_by) VALUES (?, ?, ?)",
            (username, title, added_by),
        )
        return cur.rowcount > 0

def db_remove_group(username: str) -> bool:
    with db() as conn:
        cur = conn.execute("DELETE FROM groups WHERE username = ?", (username,))
        return cur.rowcount > 0

def db_update_group_title(username: str, title: str) -> bool:
    """Update title only if currently empty / placeholder. Returns True if changed."""
    if not title:
        return False
    with db() as conn:
        cur = conn.execute(
            "UPDATE groups SET title = ? WHERE username = ? AND (title IS NULL OR title = '' OR title = ?)",
            (title, username, username),
        )
        return cur.rowcount > 0

# ---------- filters ----------
ALLOWED_FILTER_FIELDS = {
    "weight_min", "weight_max", "truck_type", "require_phone",
    "keywords_include", "keywords_exclude", "notifications_enabled",
}

def db_get_filters(chat_id: int) -> dict:
    defaults = {
        "weight_min": None,
        "weight_max": None,
        "truck_type": None,
        "require_phone": 0,
        "keywords_include": "",
        "keywords_exclude": "",
        "notifications_enabled": 1,
    }
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM user_filters WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
    if not row:
        return defaults
    return {
        "weight_min": row["weight_min"],
        "weight_max": row["weight_max"],
        "truck_type": row["truck_type"],
        "require_phone": int(row["require_phone"] or 0),
        "keywords_include": row["keywords_include"] or "",
        "keywords_exclude": row["keywords_exclude"] or "",
        "notifications_enabled": int(row["notifications_enabled"]) if row["notifications_enabled"] is not None else 1,
    }

def db_update_filter(chat_id: int, **fields):
    fields = {k: v for k, v in fields.items() if k in ALLOWED_FILTER_FIELDS}
    if not fields:
        return
    for k in ("truck_type", "keywords_include", "keywords_exclude"):
        if k in fields and isinstance(fields[k], str):
            fields[k] = fields[k][:200]
    with db() as conn:
        conn.execute("INSERT OR IGNORE INTO user_filters (chat_id) VALUES (?)", (chat_id,))
        cols = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [chat_id]
        conn.execute(
            f"UPDATE user_filters SET {cols}, updated_at = CURRENT_TIMESTAMP WHERE chat_id = ?",
            values,
        )
    _invalidate_routes_cache_safe()

def db_clear_all_filters(chat_id: int):
    with db() as conn:
        conn.execute("""
            UPDATE user_filters SET
                weight_min = NULL,
                weight_max = NULL,
                truck_type = NULL,
                require_phone = 0,
                keywords_include = '',
                keywords_exclude = '',
                updated_at = CURRENT_TIMESTAMP
            WHERE chat_id = ?
        """, (chat_id,))
    _invalidate_routes_cache_safe()

# ---------- stats ----------
def db_increment_searches(chat_id: int):
    with db() as conn:
        conn.execute(
            "UPDATE users SET total_searches = total_searches + 1, updated_at = CURRENT_TIMESTAMP WHERE chat_id = ?",
            (chat_id,),
        )

def db_increment_cargos_received(chat_id: int, count: int = 1):
    if count <= 0:
        return
    with db() as conn:
        conn.execute(
            "UPDATE users SET total_cargos_received = total_cargos_received + ? WHERE chat_id = ?",
            (count, chat_id),
        )

def db_get_user_stats(chat_id: int) -> dict:
    with db() as conn:
        row = conn.execute(
            "SELECT total_searches, total_cargos_received, from_city, to_city, language FROM users WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
    if not row:
        return {"searches": 0, "cargos": 0, "from_city": None, "to_city": None, "language": DEFAULT_LANGUAGE}
    return {
        "searches": row["total_searches"] or 0,
        "cargos": row["total_cargos_received"] or 0,
        "from_city": row["from_city"],
        "to_city": row["to_city"],
        "language": row["language"] or DEFAULT_LANGUAGE,
    }

def db_get_admin_stats() -> dict:
    cutoff_24h = (datetime.now(UZ_TIME) - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    with db() as conn:
        total = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        authorized = conn.execute("SELECT COUNT(*) AS c FROM users WHERE is_authorized = 1").fetchone()["c"]
        with_routes = conn.execute("""
            SELECT COUNT(*) AS c FROM users
            WHERE is_authorized = 1 AND from_city IS NOT NULL AND to_city IS NOT NULL
        """).fetchone()["c"]
        active_24h = conn.execute(
            "SELECT COUNT(*) AS c FROM users WHERE updated_at >= ?",
            (cutoff_24h,),
        ).fetchone()["c"]
        groups = conn.execute("SELECT COUNT(*) AS c FROM groups").fetchone()["c"]
    return {
        "total": total,
        "authorized": authorized,
        "with_routes": with_routes,
        "active_24h": active_24h,
        "groups": groups,
    }

def db_get_authorized_chat_ids():
    with db() as conn:
        rows = conn.execute("SELECT chat_id FROM users WHERE is_authorized = 1").fetchall()
    return [r["chat_id"] for r in rows]

# ============================================================
# IN-MEMORY GROUP CACHE (hot path)
# ============================================================
_groups_cache_set = None
_groups_cache_ts = 0.0
_GROUPS_CACHE_TTL = 30.0

def get_group_usernames_cached():
    global _groups_cache_set, _groups_cache_ts
    now = time.monotonic()
    if _groups_cache_set is None or now - _groups_cache_ts > _GROUPS_CACHE_TTL:
        _groups_cache_set = db_get_group_usernames()
        _groups_cache_ts = now
    return _groups_cache_set

def invalidate_groups_cache():
    global _groups_cache_set
    _groups_cache_set = None

# Cache active routes (with filters) for the live handler hot path.
# Refreshed every 10 seconds, invalidated on user/route/filter changes.
_active_routes_cache = None
_active_routes_cache_ts = 0.0
_ACTIVE_ROUTES_CACHE_TTL = 10.0

def get_active_routes_cached():
    global _active_routes_cache, _active_routes_cache_ts
    now = time.monotonic()
    if _active_routes_cache is None or now - _active_routes_cache_ts > _ACTIVE_ROUTES_CACHE_TTL:
        _active_routes_cache = db_get_active_routes_with_filters()
        _active_routes_cache_ts = now
    return _active_routes_cache

def invalidate_active_routes_cache():
    global _active_routes_cache
    _active_routes_cache = None

# ============================================================
# TEXT HELPERS
# ============================================================
BLOCK_RE = re.compile(r"\n{2,}|━+|➖+|—{2,}|={2,}|\*{3,}|═+|▬+|■+|▪{2,}|◆{2,}")
# Sequence of 2+ same non-word/non-space chars (with optional spaces between).
# Catches separators like: 🔴🔴, 🔴🔴🔴, 🔴 🔴 🔴, 🟢🟢, ▫▫▫, ......, ~~~~ etc.
SEPARATOR_RUN_RE = re.compile(r"([^\w\s])(?:\s*\1){1,}")
ARROW_RE = re.compile(r"\s*[➡➜→⇒⟶➤▶►⇨>]+\s*")
DASH_SEP_RE = re.compile(r"\s+[-—–]\s+")
PHONE_RE = re.compile(r"(?:\+|00)?\d[\d\s\-()]{7,16}\d")
GROUP_USERNAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{3,31}$")
LOGIN_RE = re.compile(r"^[a-zA-Z0-9_]{3,32}$")
MIN_PASSWORD_LENGTH = 4
WEIGHT_RANGE_RE = re.compile(
    r"\b(\d+(?:[.,]\d+)?)\s*[-–—]\s*(\d+(?:[.,]\d+)?)\s*(?:тонн[аы]?|т|tonn[ау]?|ton|t)\b",
    re.IGNORECASE,
)
WEIGHT_RE = re.compile(
    r"\b(\d+(?:[.,]\d+)?)\s*(?:тонн[аы]?|т|tonn[ау]?|ton|t)\b",
    re.IGNORECASE,
)

def normalize_text(text: str) -> str:
    text = text.lower()
    # Strip apostrophes/diacriticals so "farg'ona" == "fargona"
    for ch in "'`ʼʻ‘’":
        text = text.replace(ch, "")
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

# --- City aliases (cross-script smart matching) ---
# Each canonical key maps to a list of variants found in cargo posts
# (Uzbek Latin, Uzbek Cyrillic, Russian, English, common abbreviations).
CITY_ALIASES = {
    # Uzbekistan
    "toshkent":     ["toshkent", "tashkent", "ташкент", "тошкент"],
    "buxoro":       ["buxoro", "bukhara", "buxara", "бухоро", "бухара"],
    "samarqand":    ["samarqand", "samarkand", "самарканд", "самарқанд"],
    "andijon":      ["andijon", "andijan", "андижон", "андижан"],
    "fargona":      ["fargona", "fergana", "ferghana", "фарғона", "фергана"],
    "namangan":     ["namangan", "наманган"],
    "termiz":       ["termiz", "termez", "термиз", "термез"],
    "nukus":        ["nukus", "nokis", "нукус"],
    "xorazm":       ["xorazm", "khorezm", "khoresm", "хоразм", "хорезм"],
    "qarshi":       ["qarshi", "karshi", "қарши", "карши"],
    "jizzax":       ["jizzax", "jizzakh", "djizak", "жиззах"],
    "navoi":        ["navoiy", "navoi", "навоий", "навои"],
    "guliston":     ["guliston", "гулистан", "гулистон"],
    "urganch":      ["urganch", "ургенч", "урганч"],
    "qoqon":        ["qoqon", "kokand", "коканд", "қўқон"],
    "margilon":     ["margilon", "margilan", "марғилон", "маргилан"],
    "olot":         ["olot", "олот", "алат"],
    "shahrisabz":   ["shahrisabz", "shaxrisabz", "шахрисабз"],
    "asaka":        ["asaka", "асака"],
    "denov":        ["denov", "denau", "денау", "денов"],
    "vodiy":        ["vodiy", "водий"],
    # Russia (most common cargo destinations)
    "moskva":       ["moskva", "moscow", "москва", "msk"],
    "spb":          ["sankt peterburg", "spb", "санкт петербург", "питер"],
    "kazan":        ["kazan", "qazan", "казань", "казан"],
    "yekaterinburg":["ekaterinburg", "yekaterinburg", "ekb", "екатеринбург"],
    "novosibirsk":  ["novosibirsk", "новосибирск"],
    "krasnodar":    ["krasnodar", "краснодар"],
    "sochi":        ["sochi", "сочи"],
    "ufa":          ["ufa", "уфа"],
    "samara":       ["samara", "самара"],
    "perm":         ["perm", "пермь"],
    "rostov":       ["rostov", "ростов"],
    "volgograd":    ["volgograd", "волгоград"],
    "voronezh":     ["voronezh", "воронеж"],
    "chelyabinsk":  ["chelyabinsk", "челябинск"],
    "orenburg":     ["orenburg", "оренбург"],
    "saratov":      ["saratov", "саратов"],
    "tomsk":        ["tomsk", "томск"],
    "tula":         ["tula", "тула"],
    "tver":         ["tver", "тверь"],
    "ryazan":       ["ryazan", "рязань"],
    "yaroslavl":    ["yaroslavl", "ярославль"],
    "ivanovo":      ["ivanovo", "иваново"],
    "kostroma":     ["kostroma", "кострома"],
    "vladimir":     ["vladimir", "владимир"],
    "kaluga":       ["kaluga", "калуга"],
    "smolensk":     ["smolensk", "смоленск"],
    "bryansk":      ["bryansk", "брянск"],
    "kursk":        ["kursk", "курск"],
    "lipetsk":      ["lipetsk", "липецк"],
    "tambov":       ["tambov", "тамбов"],
    "belgorod":     ["belgorod", "белгород"],
    "novgorod":     ["novgorod", "новгород"],
    "pskov":        ["pskov", "псков"],
    "vologda":      ["vologda", "вологда"],
    "arkhangelsk":  ["arkhangelsk", "архангельск"],
    "murmansk":     ["murmansk", "мурманск"],
    "nn":           ["nizhny novgorod", "нижний новгород"],
    "egoryevsk":    ["egoryevsk", "егорьевск"],
    "cherepovets":  ["cherepovets", "череповец"],
    "murom":        ["murom", "муром"],
    "shatura":      ["shatura", "шатура"],
    "sharya":       ["sharya", "шарья"],
    # Tajikistan
    "dushanbe":     ["dushanbe", "душанбе"],
    "khujand":      ["khujand", "khudjand", "худжанд", "хужанд"],
    # Kazakhstan
    "almaty":       ["almaty", "алматы"],
    "astana":       ["astana", "астана"],
    "shymkent":     ["shymkent", "chimkent", "шымкент", "чимкент"],
    "aktau":        ["aktau", "актау"],
    "atyrau":       ["atyrau", "атырау"],
    "karaganda":    ["karaganda", "караганда"],
    "pavlodar":     ["pavlodar", "павлодар"],
    # Belarus
    "minsk":        ["minsk", "минск"],
    "mogilev":      ["mogilev", "могилев", "могилёв"],
    "gomel":        ["gomel", "гомель"],
    "vitebsk":      ["vitebsk", "витебск"],
    "grodno":       ["grodno", "гродно"],
    # Kyrgyzstan
    "bishkek":      ["bishkek", "бишкек"],
    "osh":          ["osh", "ош"],
}

# Reverse map: every alias variant → canonical key
ALIAS_TO_CANONICAL = {}
for _canonical, _aliases in CITY_ALIASES.items():
    for _alias in _aliases:
        ALIAS_TO_CANONICAL[normalize_text(_alias)] = _canonical
    ALIAS_TO_CANONICAL[normalize_text(_canonical)] = _canonical

def canonicalize_city(name: str):
    """Map any input variant to a canonical key, or None if unknown."""
    if not name:
        return None
    n = normalize_text(name)
    if not n:
        return None
    if n in ALIAS_TO_CANONICAL:
        return ALIAS_TO_CANONICAL[n]
    first = n.split()[0] if n else ""
    if first in ALIAS_TO_CANONICAL:
        return ALIAS_TO_CANONICAL[first]
    return None

def expand_aliases(city: str):
    """Returns list of variant strings to search for; falls back to literal input."""
    canonical = canonicalize_city(city)
    if canonical and canonical in CITY_ALIASES:
        return CITY_ALIASES[canonical]
    return [city] if city else []

def cargo_hash(text: str) -> str:
    return hashlib.sha256(normalize_text(text).encode("utf-8")).hexdigest()

def split_blocks(text: str):
    # First normalize repeated emoji/symbol separator runs (🔴🔴🔴 etc.) to \n\n,
    # then split on the standard block separators.
    text = SEPARATOR_RUN_RE.sub("\n\n", text)
    return [b.strip() for b in BLOCK_RE.split(text) if b.strip()]

def parse_user_route(text: str):
    text = (text or "").strip()
    if not text:
        return None
    parts = None
    if ARROW_RE.search(text):
        parts = ARROW_RE.split(text)
    if (not parts or len(parts) < 2) and DASH_SEP_RE.search(text):
        parts = DASH_SEP_RE.split(text)
    if not parts or len(parts) < 2:
        words = text.split()
        if len(words) >= 2:
            parts = [words[0], words[-1]]
    if not parts or len(parts) < 2:
        return None
    a, b = parts[0].strip(), parts[1].strip()
    return (a, b) if a and b else None

def city_stem(city: str) -> str:
    words = normalize_text(city).split()
    if not words:
        return ""
    word = words[0]
    cut = min(len(word), max(4, len(word) - 2))
    return word[:cut]

def find_position(text_norm: str, stem: str) -> int:
    if not stem:
        return -1
    pattern = re.compile(rf"\b{re.escape(stem)}\w*")
    m = pattern.search(text_norm)
    return m.start() if m else -1

def match_route(text: str, from_city: str, to_city: str) -> bool:
    """Direction-aware match using cross-script city aliases (Cyrillic / Latin / Russian).
    For known cities, every variant is searched. Stem matching is used for declension."""
    from_canonical = canonicalize_city(from_city)
    to_canonical = canonicalize_city(to_city)
    if from_canonical and to_canonical and from_canonical == to_canonical:
        return False

    from_aliases = expand_aliases(from_city)
    to_aliases = expand_aliases(to_city)
    if not from_aliases or not to_aliases:
        return False

    norm = normalize_text(text)

    # Earliest position of any from_alias
    from_pos = -1
    for alias in from_aliases:
        stem = city_stem(alias)
        if not stem:
            continue
        pos = find_position(norm, stem)
        if pos != -1 and (from_pos == -1 or pos < from_pos):
            from_pos = pos
    if from_pos == -1:
        return False

    # Earliest position of any to_alias AFTER from_pos
    for alias in to_aliases:
        stem = city_stem(alias)
        if not stem:
            continue
        try:
            pattern = re.compile(rf"\b{re.escape(stem)}\w*", re.UNICODE)
        except re.error:
            continue
        m = pattern.search(norm, from_pos + 1)
        if m:
            return True
    return False

def extract_phone(text: str):
    m = PHONE_RE.search(text)
    if not m:
        return None
    raw = m.group(0)
    digits = re.sub(r"\D", "", raw)
    if not (9 <= len(digits) <= 15):
        return None
    return re.sub(r"[\s\-()]", "", raw)

def extract_weight_range(text: str):
    """Returns (min, max) in tons, or None."""
    m = WEIGHT_RANGE_RE.search(text)
    if m:
        try:
            a = float(m.group(1).replace(",", "."))
            b = float(m.group(2).replace(",", "."))
            return (min(a, b), max(a, b))
        except ValueError:
            pass
    m = WEIGHT_RE.search(text)
    if m:
        try:
            v = float(m.group(1).replace(",", "."))
            return (v, v)
        except ValueError:
            pass
    return None

def format_username(sender, lang: str) -> str:
    if sender is None:
        return t("unknown_user", lang)
    # Plain string (bot-only mode stores sender_name/username as text)
    if isinstance(sender, str):
        s = sender.strip()
        if not s:
            return t("unknown_user", lang)
        return s if s.startswith("@") else s
    # Telethon entity (legacy path)
    if getattr(sender, "username", None):
        return f"@{sender.username}"
    if getattr(sender, "first_name", None):
        return sender.first_name
    return t("unknown_user", lang)

def normalize_group_username(text: str):
    if not text:
        return None
    s = text.strip()
    s = re.sub(r"^https?://(t\.me|telegram\.me)/", "", s, flags=re.IGNORECASE)
    s = s.lstrip("@").rstrip("/").strip()
    if not s or not GROUP_USERNAME_RE.match(s):
        return None
    return s.lower()

# ============================================================
# FILTER LOGIC
# ============================================================
def parse_keyword_list(s: str):
    if not s:
        return []
    return [w.strip().lower() for w in s.split(",") if w.strip()]

def cargo_passes_filters(block_text: str, phone, filters: dict) -> bool:
    """Apply user filters to a single cargo block. True = passes."""
    text_norm = normalize_text(block_text)

    # Weight range (range cargo passes when overlapping with filter range)
    if filters.get("weight_min") is not None or filters.get("weight_max") is not None:
        weight = extract_weight_range(block_text)
        if weight is None:
            return False
        wmin, wmax = weight
        if filters.get("weight_min") is not None and wmax < filters["weight_min"]:
            return False
        if filters.get("weight_max") is not None and wmin > filters["weight_max"]:
            return False

    # Truck type
    if filters.get("truck_type"):
        truck_norm = normalize_text(filters["truck_type"])
        if truck_norm and truck_norm not in text_norm:
            return False

    # Phone required
    if filters.get("require_phone") and not phone:
        return False

    # Include keywords (all must be present)
    include = parse_keyword_list(filters.get("keywords_include", ""))
    if include:
        for kw in include:
            if kw not in text_norm:
                return False

    # Exclude keywords (none allowed)
    exclude = parse_keyword_list(filters.get("keywords_exclude", ""))
    if exclude:
        for kw in exclude:
            if kw in text_norm:
                return False

    return True

def format_filters_view(filters: dict, lang: str) -> str:
    """Pretty-print user filters; returns None if no filters set."""
    parts = [t("filters_show_header", lang)]
    has_any = False

    if filters.get("weight_min") is not None or filters.get("weight_max") is not None:
        wmin = filters.get("weight_min")
        wmax = filters.get("weight_max")
        if wmin is not None and wmax is not None:
            value = f"{format_num(wmin)}–{format_num(wmax)}"
        elif wmin is not None:
            value = f"≥ {format_num(wmin)}"
        else:
            value = f"≤ {format_num(wmax)}"
        parts.append(t("filter_weight_set", lang, value=value))
        has_any = True

    if filters.get("truck_type"):
        parts.append(t("filter_truck_set", lang, value=filters["truck_type"]))
        has_any = True

    if filters.get("require_phone"):
        parts.append(t("filter_phone_required", lang, value=t("filter_yes", lang)))
        has_any = True

    if filters.get("keywords_include"):
        parts.append(t("filter_include_set", lang, value=filters["keywords_include"]))
        has_any = True

    if filters.get("keywords_exclude"):
        parts.append(t("filter_exclude_set", lang, value=filters["keywords_exclude"]))
        has_any = True

    if not has_any:
        return None

    parts.append(t("filters_footer", lang))
    return "\n".join(parts)

def format_num(x):
    if x is None:
        return "-"
    if abs(x - round(x)) < 1e-9:
        return str(int(round(x)))
    return f"{x:g}"

def parse_weight_filter_value(value: str):
    """
    Returns one of:
      ("clear",), ("min", n), ("max", n), ("range", a, b), or None (invalid).
    """
    v = (value or "").strip().lower()
    if v == "clear":
        return ("clear",)
    m = re.match(r"^min\s+(\d+(?:[.,]\d+)?)$", v)
    if m:
        return ("min", float(m.group(1).replace(",", ".")))
    m = re.match(r"^max\s+(\d+(?:[.,]\d+)?)$", v)
    if m:
        return ("max", float(m.group(1).replace(",", ".")))
    m = re.match(r"^(\d+(?:[.,]\d+)?)\s*[-–—]\s*(\d+(?:[.,]\d+)?)$", v)
    if m:
        a = float(m.group(1).replace(",", "."))
        b = float(m.group(2).replace(",", "."))
        return ("range", min(a, b), max(a, b))
    m = re.match(r"^(\d+(?:[.,]\d+)?)$", v)
    if m:
        n = float(m.group(1).replace(",", "."))
        return ("range", n, n)
    return None

def parse_on_off(value: str):
    v = (value or "").strip().lower()
    if v in ("on", "yes", "1", "ha", "да"):
        return True
    if v in ("off", "no", "0", "yo'q", "yoq", "нет"):
        return False
    return None

# ============================================================
# TTL CACHE (search results)
# ============================================================
class TTLCache:
    def __init__(self, ttl_seconds: int):
        self.ttl = ttl_seconds
        self._data: dict = {}

    def get(self, key):
        item = self._data.get(key)
        if not item:
            return None
        ts, value = item
        if time.monotonic() - ts > self.ttl:
            self._data.pop(key, None)
            return None
        return value

    def set(self, key, value):
        self._data[key] = (time.monotonic(), value)

    def invalidate_all(self):
        self._data.clear()

    def cleanup(self):
        now = time.monotonic()
        expired = [k for k, (ts, _) in self._data.items() if now - ts > self.ttl]
        for k in expired:
            self._data.pop(k, None)

search_cache = TTLCache(SEARCH_CACHE_TTL_SECONDS)

# ============================================================
# BOT / CLIENT
# ============================================================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
HTML_PARSE = "HTML"

class AuthStates(StatesGroup):
    waiting_for_language = State()
    waiting_for_phone = State()
    access_choice = State()
    waiting_for_login = State()
    waiting_for_password = State()
    changing_language = State()

class FormStates(StatesGroup):
    # Route
    setting_route = State()
    # Filters
    setting_weight = State()
    setting_truck = State()
    setting_include = State()
    setting_exclude = State()
    # Settings
    changing_password = State()
    # Admin: account creation
    add_user_login = State()
    add_user_password = State()
    add_user_admin_choice = State()  # awaits inline button
    # Admin: reset password
    reset_pass_for_login = State()
    # Admin: groups
    add_group_input = State()
    # Admin: broadcast
    broadcast_input = State()
    # Messaging
    messaging_admin = State()      # regular user → admin
    replying_to_user = State()     # admin → user (user_chat_id in FSM data)
    # Auto-routes
    adding_auto_route = State()    # entering a new route to subscribe to

async def send_main_menu(chat_id: int, lang: str, is_admin: bool, hint: bool = True):
    text = t("main_menu_hint", lang) if hint else "."
    await bot.send_message(
        chat_id, text,
        reply_markup=main_menu_keyboard(lang, is_admin),
        parse_mode=HTML_PARSE,
    )

# ============================================================
# HANDLERS: language / start / logout / help
# ============================================================
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        TRANSLATIONS["uz"]["choose_language"],
        reply_markup=language_keyboard(),
    )
    await state.set_state(AuthStates.waiting_for_language)

@dp.message(AuthStates.waiting_for_language)
async def process_initial_language(message: types.Message, state: FSMContext):
    lang = parse_language_choice(message.text or "")
    if not lang:
        await message.answer(
            TRANSLATIONS["uz"]["choose_language"],
            reply_markup=language_keyboard(),
        )
        return
    try:
        db_set_language(message.chat.id, lang)
        db_set_profile(
            message.chat.id,
            first_name=getattr(message.from_user, "first_name", None),
            telegram_username=getattr(message.from_user, "username", None),
        )
    except Exception:
        log.exception("Failed to save language/profile")

    if db_is_authorized(message.chat.id):
        await state.clear()
        await message.answer(t("language_set", lang), reply_markup=ReplyKeyboardRemove())
        await send_main_menu(message.chat.id, lang, db_is_admin(message.chat.id))
        return

    # Not authorized — record as pending, notify admins, ask for phone
    try:
        db_record_pending(message.chat.id, _user_display_name(message.from_user), lang)
    except Exception:
        log.exception("Failed to record pending user")

    if not db_get_phone(message.chat.id):
        await message.answer(t("phone_request", lang), reply_markup=phone_request_keyboard(lang))
        await state.set_state(AuthStates.waiting_for_phone)
    else:
        await _proceed_to_login(message, lang, state)

@dp.message(AuthStates.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    phone = None
    if message.contact and message.contact.phone_number:
        phone = message.contact.phone_number
    elif message.text:
        if is_skip_phone_text(message.text):
            phone = None
        else:
            cleaned = re.sub(r"[^\d+]", "", message.text)
            if 9 <= len(re.sub(r"\D", "", cleaned)) <= 15:
                phone = cleaned

    if phone:
        try:
            db_set_phone(chat_id, phone)
        except Exception:
            log.exception("Failed to save phone")
        await message.answer(t("phone_received", lang, phone=phone),
                             reply_markup=ReplyKeyboardRemove())
    else:
        await message.answer(t("phone_skipped", lang), reply_markup=ReplyKeyboardRemove())

    # Notify admins now (with phone if provided)
    try:
        await _notify_admins_new_pending(chat_id, message.from_user, lang)
    except Exception:
        log.exception("Failed to notify admins")

    await _show_access_choice(message.chat.id, lang, state)

async def _show_access_choice(chat_id: int, lang: str, state: FSMContext):
    """Unauthorized users see this after phone capture: choose to login or message admin."""
    await bot.send_message(
        chat_id,
        t("access_no_permission", lang),
        parse_mode=HTML_PARSE,
        reply_markup=access_request_keyboard(lang),
    )
    await state.set_state(AuthStates.access_choice)

@dp.message(AuthStates.access_choice)
async def process_access_choice(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    text = (message.text or "").strip()

    if is_access_button_login(text):
        await message.answer(t("welcome", lang), reply_markup=ReplyKeyboardRemove())
        await message.answer(t("enter_login", lang))
        await state.set_state(AuthStates.waiting_for_login)
        return

    if is_access_button_msg_admin(text):
        if not db_admin_chat_ids():
            await message.answer(t("message_no_admins", lang))
            return
        await state.set_state(FormStates.messaging_admin)
        await message.answer(
            t("form_enter_message_to_admin", lang),
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    # Unknown text → re-show keyboard
    await message.answer(
        t("access_no_permission", lang),
        parse_mode=HTML_PARSE,
        reply_markup=access_request_keyboard(lang),
    )

def _user_display_name(user) -> str:
    if not user:
        return "?"
    if getattr(user, "username", None):
        return f"@{user.username}"
    parts = [getattr(user, "first_name", None), getattr(user, "last_name", None)]
    name = " ".join(p for p in parts if p)
    return name or f"id{user.id}"

async def _notify_admins_new_pending(chat_id: int, user, user_lang: str):
    name = _user_display_name(user)
    phone = db_get_phone(chat_id) or "-"
    for admin_chat in db_admin_chat_ids():
        try:
            admin_lang = db_get_language(admin_chat)
            text = t("admin_new_pending_notification", admin_lang,
                     name=name, chat_id=chat_id)
            text += f"\n📞 {phone}"
            await bot.send_message(admin_chat, text, parse_mode=HTML_PARSE)
        except Exception:
            log.exception("Failed to notify admin %s about pending %s", admin_chat, chat_id)

@dp.message(Command("lang"))
async def cmd_lang(message: types.Message, state: FSMContext):
    lang = db_get_language(message.chat.id)
    await state.clear()
    await message.answer(t("lang_prompt", lang), reply_markup=language_keyboard())
    await state.set_state(AuthStates.changing_language)

@dp.message(AuthStates.changing_language)
async def process_change_language(message: types.Message, state: FSMContext):
    lang = parse_language_choice(message.text or "")
    if not lang:
        old_lang = db_get_language(message.chat.id)
        await message.answer(t("lang_prompt", old_lang), reply_markup=language_keyboard())
        return
    try:
        db_set_language(message.chat.id, lang)
    except Exception:
        log.exception("Failed to change language")
    await state.clear()
    await message.answer(t("language_set", lang), reply_markup=ReplyKeyboardRemove())

@dp.message(Command("logout"))
async def cmd_logout(message: types.Message, state: FSMContext):
    lang = db_get_language(message.chat.id)
    await state.clear()
    try:
        db_set_unauthorized(message.chat.id)
    except Exception:
        log.exception("logout: db error")
    await message.answer(t("logout", lang), reply_markup=ReplyKeyboardRemove())

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    lang = db_get_language(message.chat.id)
    text = t("help_user", lang)
    if db_is_admin(message.chat.id):
        text += t("help_admin", lang)
    await message.answer(text, parse_mode=HTML_PARSE)

# ============================================================
# HANDLERS: auth flow
# ============================================================
@dp.message(AuthStates.waiting_for_login)
async def process_login(message: types.Message, state: FSMContext):
    lang = db_get_language(message.chat.id)
    login = (message.text or "").strip()
    if not login:
        await message.answer(t("login_empty", lang))
        return
    await state.update_data(login=login)
    await message.answer(t("enter_password", lang))
    await state.set_state(AuthStates.waiting_for_password)

@dp.message(AuthStates.waiting_for_password)
async def process_password(message: types.Message, state: FSMContext):
    lang = db_get_language(message.chat.id)
    data = await state.get_data()
    login = data.get("login", "")
    password = (message.text or "").strip()

    try:
        ok, _is_admin = db_check_credentials(login, password)
    except Exception:
        log.exception("Auth check failed")
        await state.clear()
        await message.answer(t("internal_error", lang))
        return

    if ok:
        try:
            db_set_authorized(message.chat.id, login, lang)
        except Exception:
            log.exception("Failed to set authorized")
            await state.clear()
            await message.answer(t("internal_error", lang))
            return
        await state.clear()
        await message.answer(t("auth_success", lang), parse_mode=HTML_PARSE)
        await send_main_menu(message.chat.id, lang, db_is_admin(message.chat.id), hint=False)
        log.info("Login OK chat=%s login='%s'", message.chat.id, login)
    else:
        await state.set_state(AuthStates.waiting_for_login)
        await message.answer(t("auth_failed", lang))
        log.warning("Login FAIL chat=%s login='%s'", message.chat.id, login)

# ============================================================
# HANDLERS: route management
# ============================================================
def _need_auth(message: types.Message, lang: str) -> bool:
    if not db_is_authorized(message.chat.id):
        asyncio.create_task(message.answer(t("not_authorized", lang)))
        return False
    return True

@dp.message(Command("myroute"))
async def cmd_myroute(message: types.Message):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    if not db_is_authorized(chat_id):
        await message.answer(t("not_authorized", lang))
        return
    route = db_get_route(chat_id)
    if not route:
        await message.answer(t("myroute_none", lang))
        return
    filters = db_get_filters(chat_id)
    notify_text = t("notify_status_on", lang) if filters["notifications_enabled"] else t("notify_status_off", lang)
    await message.answer(
        t("myroute_show", lang, from_city=route[0], to_city=route[1], notify=notify_text),
        parse_mode=HTML_PARSE,
    )

@dp.message(Command("clearroute"))
async def cmd_clearroute(message: types.Message):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    if not db_is_authorized(chat_id):
        await message.answer(t("not_authorized", lang))
        return
    try:
        db_clear_route(chat_id)
    except Exception:
        log.exception("clearroute failed")
        await message.answer(t("internal_error", lang))
        return
    await message.answer(t("clearroute_done", lang))

@dp.message(Command("notify"))
async def cmd_notify(message: types.Message, command: CommandObject):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    if not db_is_authorized(chat_id):
        await message.answer(t("not_authorized", lang))
        return
    val = parse_on_off(command.args or "")
    if val is None:
        await message.answer(t("notify_usage", lang))
        return
    try:
        db_update_filter(chat_id, notifications_enabled=1 if val else 0)
    except Exception:
        log.exception("notify update failed")
        await message.answer(t("internal_error", lang))
        return
    await message.answer(t("notify_on" if val else "notify_off", lang))

# ============================================================
# HANDLERS: filters
# ============================================================
@dp.message(Command("filters"))
async def cmd_filters(message: types.Message):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    if not db_is_authorized(chat_id):
        await message.answer(t("not_authorized", lang))
        return
    filters = db_get_filters(chat_id)
    view = format_filters_view(filters, lang)
    if view is None:
        await message.answer(t("filters_none", lang), parse_mode=HTML_PARSE)
    else:
        await message.answer(view, parse_mode=HTML_PARSE)

@dp.message(Command("setfilter"))
async def cmd_setfilter(message: types.Message, command: CommandObject):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    if not db_is_authorized(chat_id):
        await message.answer(t("not_authorized", lang))
        return
    args = (command.args or "").strip()
    if not args:
        await message.answer(t("setfilter_usage", lang), parse_mode=HTML_PARSE)
        return
    parts = args.split(maxsplit=1)
    filter_type = parts[0].lower()
    value = parts[1].strip() if len(parts) > 1 else ""

    try:
        if filter_type == "weight":
            parsed = parse_weight_filter_value(value)
            if parsed is None:
                await message.answer(t("filter_weight_usage", lang), parse_mode=HTML_PARSE)
                return
            if parsed[0] == "clear":
                db_update_filter(chat_id, weight_min=None, weight_max=None)
                await message.answer(t("filter_cleared", lang))
                return
            if parsed[0] == "min":
                db_update_filter(chat_id, weight_min=parsed[1])
            elif parsed[0] == "max":
                db_update_filter(chat_id, weight_max=parsed[1])
            elif parsed[0] == "range":
                db_update_filter(chat_id, weight_min=parsed[1], weight_max=parsed[2])
            await message.answer(t("filter_updated", lang))

        elif filter_type == "truck":
            if value.lower() == "clear" or not value:
                db_update_filter(chat_id, truck_type=None)
                await message.answer(t("filter_cleared", lang))
            else:
                db_update_filter(chat_id, truck_type=value)
                await message.answer(t("filter_updated", lang))

        elif filter_type == "phone":
            on_off = parse_on_off(value)
            if on_off is None:
                await message.answer(t("filter_phone_usage", lang), parse_mode=HTML_PARSE)
                return
            db_update_filter(chat_id, require_phone=1 if on_off else 0)
            await message.answer(t("filter_updated", lang))

        elif filter_type == "include":
            if value.lower() == "clear" or not value:
                db_update_filter(chat_id, keywords_include="")
                await message.answer(t("filter_cleared", lang))
            else:
                db_update_filter(chat_id, keywords_include=value)
                await message.answer(t("filter_updated", lang))

        elif filter_type == "exclude":
            if value.lower() == "clear" or not value:
                db_update_filter(chat_id, keywords_exclude="")
                await message.answer(t("filter_cleared", lang))
            else:
                db_update_filter(chat_id, keywords_exclude=value)
                await message.answer(t("filter_updated", lang))

        else:
            await message.answer(t("setfilter_unknown", lang, type=filter_type))
            return
    except Exception:
        log.exception("setfilter failed")
        await message.answer(t("internal_error", lang))
        return

    search_cache.invalidate_all()  # filters changed, clear cached results

@dp.message(Command("clearfilters"))
async def cmd_clearfilters(message: types.Message):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    if not db_is_authorized(chat_id):
        await message.answer(t("not_authorized", lang))
        return
    try:
        db_clear_all_filters(chat_id)
    except Exception:
        log.exception("clearfilters failed")
        await message.answer(t("internal_error", lang))
        return
    search_cache.invalidate_all()
    await message.answer(t("clearfilters_done", lang))

# ============================================================
# HANDLERS: stats
# ============================================================
@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    if not db_is_authorized(chat_id):
        await message.answer(t("not_authorized", lang))
        return
    stats = db_get_user_stats(chat_id)
    if stats["from_city"] and stats["to_city"]:
        route_text = f"{stats['from_city']} ➜ {stats['to_city']}"
    else:
        route_text = t("stats_no_route", lang)
    filters = db_get_filters(chat_id)
    notify_text = t("notify_status_on", lang) if filters["notifications_enabled"] else t("notify_status_off", lang)
    await message.answer(
        t("stats_user", lang,
          searches=stats["searches"],
          cargos=stats["cargos"],
          route=route_text,
          notify=notify_text,
          language=stats["language"]),
        parse_mode=HTML_PARSE,
    )

# ============================================================
# HANDLERS: admin (groups / users / broadcast)
# ============================================================
@dp.message(Command("groups"))
async def cmd_groups(message: types.Message):
    lang = db_get_language(message.chat.id)
    if not db_is_authorized(message.chat.id):
        await message.answer(t("not_authorized", lang))
        return
    if not db_is_admin(message.chat.id):
        await message.answer(t("admin_only", lang))
        return
    groups = db_list_groups()
    if not groups:
        await message.answer(t("groups_empty", lang))
        return
    lines = [t("groups_header", lang, count=len(groups))]
    for username, title in groups:
        display = title if title else username
        lines.append(f"  • @{username} — {display}")
    await message.answer("\n".join(lines))

@dp.message(Command("addgroup"))
async def cmd_addgroup(message: types.Message, command: CommandObject):
    lang = db_get_language(message.chat.id)
    if not db_is_authorized(message.chat.id):
        await message.answer(t("not_authorized", lang))
        return
    if not db_is_admin(message.chat.id):
        await message.answer(t("admin_only", lang))
        return
    args = (command.args or "").strip()
    if not args:
        await message.answer(t("addgroup_usage", lang))
        return
    username = normalize_group_username(args)
    if not username:
        await message.answer(t("group_invalid", lang, name=args))
        return
    if username in db_get_group_usernames():
        await message.answer(t("group_already", lang, name=f"@{username}"))
        return
    title = username  # title fills in the first time the bot sees a message there
    try:
        added = db_add_group(username, title, message.chat.id)
    except Exception:
        log.exception("Failed to add group")
        await message.answer(t("internal_error", lang))
        return
    if added:
        invalidate_groups_cache()
        search_cache.invalidate_all()
        log.info("Group added: %s by chat=%s", username, message.chat.id)
        await message.answer(t("group_added", lang, name=f"@{username} — {title}"))
    else:
        await message.answer(t("group_already", lang, name=f"@{username}"))

@dp.message(Command("delgroup"))
async def cmd_delgroup(message: types.Message, command: CommandObject):
    lang = db_get_language(message.chat.id)
    if not db_is_authorized(message.chat.id):
        await message.answer(t("not_authorized", lang))
        return
    if not db_is_admin(message.chat.id):
        await message.answer(t("admin_only", lang))
        return
    args = (command.args or "").strip()
    if not args:
        await message.answer(t("delgroup_usage", lang))
        return
    username = normalize_group_username(args)
    if not username:
        await message.answer(t("group_invalid", lang, name=args))
        return
    try:
        removed = db_remove_group(username)
    except Exception:
        log.exception("Failed to remove group")
        await message.answer(t("internal_error", lang))
        return
    if removed:
        invalidate_groups_cache()
        search_cache.invalidate_all()
        log.info("Group removed: %s by chat=%s", username, message.chat.id)
        await message.answer(t("group_removed", lang, name=f"@{username}"))
    else:
        await message.answer(t("group_not_found", lang, name=f"@{username}"))

@dp.message(Command("users"))
async def cmd_users(message: types.Message):
    lang = db_get_language(message.chat.id)
    if not db_is_authorized(message.chat.id):
        await message.answer(t("not_authorized", lang))
        return
    if not db_is_admin(message.chat.id):
        await message.answer(t("admin_only", lang))
        return
    try:
        stats = db_get_admin_stats()
    except Exception:
        log.exception("admin stats failed")
        await message.answer(t("internal_error", lang))
        return
    await message.answer(t("admin_users", lang, **stats), parse_mode=HTML_PARSE)

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message, command: CommandObject):
    lang = db_get_language(message.chat.id)
    if not db_is_authorized(message.chat.id):
        await message.answer(t("not_authorized", lang))
        return
    if not db_is_admin(message.chat.id):
        await message.answer(t("admin_only", lang))
        return
    text = (command.args or "").strip()
    if not text:
        await message.answer(t("broadcast_usage", lang))
        return
    chat_ids = db_get_authorized_chat_ids()
    sent = 0
    failed = 0
    for cid in chat_ids:
        try:
            recipient_lang = db_get_language(cid)
            await bot.send_message(
                cid,
                t("broadcast_received", recipient_lang, message=text),
                parse_mode=HTML_PARSE,
            )
            sent += 1
        except Exception:
            failed += 1
            log.exception("broadcast to %s failed", cid)
        await asyncio.sleep(BROADCAST_RATE_DELAY)
    log.info("Broadcast: sent=%d failed=%d", sent, failed)
    await message.answer(t("broadcast_done", lang, sent=sent, failed=failed))

# ============================================================
# HANDLERS: account management (admin / self)
# ============================================================
def _yes_no(value: bool, lang: str) -> str:
    return t("filter_yes", lang) if value else t("filter_no", lang)

@dp.message(Command("accounts"))
async def cmd_accounts(message: types.Message):
    lang = db_get_language(message.chat.id)
    if not db_is_authorized(message.chat.id):
        await message.answer(t("not_authorized", lang))
        return
    if not db_is_admin(message.chat.id):
        await message.answer(t("admin_only", lang))
        return
    try:
        accounts = db_list_accounts()
    except Exception:
        log.exception("list accounts failed")
        await message.answer(t("internal_error", lang))
        return
    if not accounts:
        await message.answer(t("accounts_empty", lang))
        return
    lines = [t("accounts_header", lang, count=len(accounts))]
    sess_label = t("accounts_session_label", lang)
    rt_label = t("accounts_routes_label", lang)
    admin_label = t("accounts_admin_label", lang)
    for acc in accounts:
        badge = f" 👑 {admin_label}" if acc["is_admin"] else ""
        lines.append(
            f"• <code>{acc['login']}</code>{badge} — "
            f"{sess_label}: <b>{acc['sessions']}</b>, "
            f"{rt_label}: <b>{acc['routes']}</b>"
        )
    lines.append(t("accounts_footer", lang))
    await message.answer("\n".join(lines), parse_mode=HTML_PARSE)

@dp.message(Command("adduser"))
async def cmd_adduser(message: types.Message, command: CommandObject):
    lang = db_get_language(message.chat.id)
    if not db_is_authorized(message.chat.id):
        await message.answer(t("not_authorized", lang))
        return
    if not db_is_admin(message.chat.id):
        await message.answer(t("admin_only", lang))
        return
    args = (command.args or "").strip().split()
    if len(args) < 2:
        await message.answer(t("adduser_usage", lang), parse_mode=HTML_PARSE)
        return
    login = args[0]
    password = args[1]
    is_admin_flag = (len(args) >= 3 and args[2].lower() == "admin")

    if not LOGIN_RE.match(login):
        await message.answer(t("adduser_invalid_login", lang, login=login), parse_mode=HTML_PARSE)
        return
    if len(password) < MIN_PASSWORD_LENGTH:
        await message.answer(t("adduser_password_short", lang))
        return
    try:
        if not db_create_account(login, password, is_admin_flag):
            await message.answer(t("adduser_already_exists", lang, login=login), parse_mode=HTML_PARSE)
            return
    except Exception:
        log.exception("adduser failed")
        await message.answer(t("internal_error", lang))
        return
    log.info("Account created: %s (admin=%s) by chat=%s", login, is_admin_flag, message.chat.id)
    await message.answer(
        t("adduser_done", lang, login=login, password=password,
          admin=_yes_no(is_admin_flag, lang)),
        parse_mode=HTML_PARSE,
    )

@dp.message(Command("deluser"))
async def cmd_deluser(message: types.Message, command: CommandObject):
    lang = db_get_language(message.chat.id)
    if not db_is_authorized(message.chat.id):
        await message.answer(t("not_authorized", lang))
        return
    if not db_is_admin(message.chat.id):
        await message.answer(t("admin_only", lang))
        return
    login = (command.args or "").strip().split(maxsplit=1)
    login = login[0] if login else ""
    if not login:
        await message.answer(t("deluser_usage", lang))
        return
    own_login = db_get_user_login(message.chat.id)
    if own_login and login.lower() == own_login.lower():
        await message.answer(t("deluser_self", lang))
        return
    try:
        result = db_delete_account(login)
    except Exception:
        log.exception("deluser failed")
        await message.answer(t("internal_error", lang))
        return
    if result == "not_found":
        await message.answer(t("deluser_not_found", lang, login=login), parse_mode=HTML_PARSE)
    elif result == "last_admin":
        await message.answer(t("deluser_last_admin", lang))
    else:
        log.info("Account deleted: %s by chat=%s", login, message.chat.id)
        await message.answer(t("deluser_done", lang, login=login), parse_mode=HTML_PARSE)

@dp.message(Command("resetpass"))
async def cmd_resetpass(message: types.Message, command: CommandObject):
    lang = db_get_language(message.chat.id)
    if not db_is_authorized(message.chat.id):
        await message.answer(t("not_authorized", lang))
        return
    if not db_is_admin(message.chat.id):
        await message.answer(t("admin_only", lang))
        return
    args = (command.args or "").strip().split(maxsplit=1)
    if len(args) < 2:
        await message.answer(t("resetpass_usage", lang))
        return
    login, new_pass = args[0], args[1]
    if not db_login_exists(login):
        await message.answer(t("deluser_not_found", lang, login=login), parse_mode=HTML_PARSE)
        return
    if len(new_pass) < MIN_PASSWORD_LENGTH:
        await message.answer(t("adduser_password_short", lang))
        return
    try:
        db_set_password(login, new_pass, store_plaintext=True)
    except Exception:
        log.exception("resetpass failed")
        await message.answer(t("internal_error", lang))
        return
    log.info("Password reset: %s by chat=%s", login, message.chat.id)
    await message.answer(
        t("resetpass_done", lang, login=login, password=new_pass),
        parse_mode=HTML_PARSE,
    )

@dp.message(Command("changepass"))
async def cmd_changepass(message: types.Message, command: CommandObject):
    lang = db_get_language(message.chat.id)
    if not db_is_authorized(message.chat.id):
        await message.answer(t("not_authorized", lang))
        return
    new_pass = (command.args or "").strip()
    if not new_pass:
        await message.answer(t("changepass_usage", lang), parse_mode=HTML_PARSE)
        return
    if len(new_pass) < MIN_PASSWORD_LENGTH:
        await message.answer(t("changepass_short", lang))
        return
    own_login = db_get_user_login(message.chat.id)
    if not own_login:
        await message.answer(t("not_authorized", lang))
        return
    try:
        db_set_password(own_login, new_pass)
    except Exception:
        log.exception("changepass failed")
        await message.answer(t("internal_error", lang))
        return
    log.info("Password changed: %s by chat=%s", own_login, message.chat.id)
    await message.answer(t("changepass_done", lang))

@dp.message(Command("makeadmin"))
async def cmd_makeadmin(message: types.Message, command: CommandObject):
    lang = db_get_language(message.chat.id)
    if not db_is_authorized(message.chat.id):
        await message.answer(t("not_authorized", lang))
        return
    if not db_is_admin(message.chat.id):
        await message.answer(t("admin_only", lang))
        return
    args = (command.args or "").strip().split(maxsplit=1)
    login = args[0] if args else ""
    if not login:
        await message.answer(t("makeadmin_usage", lang))
        return
    try:
        result = db_set_admin_flag(login, True)
    except Exception:
        log.exception("makeadmin failed")
        await message.answer(t("internal_error", lang))
        return
    if result == "not_found":
        await message.answer(t("deluser_not_found", lang, login=login), parse_mode=HTML_PARSE)
    else:
        log.info("Made admin: %s by chat=%s", login, message.chat.id)
        await message.answer(t("makeadmin_done", lang, login=login), parse_mode=HTML_PARSE)

@dp.message(Command("unadmin"))
async def cmd_unadmin(message: types.Message, command: CommandObject):
    lang = db_get_language(message.chat.id)
    if not db_is_authorized(message.chat.id):
        await message.answer(t("not_authorized", lang))
        return
    if not db_is_admin(message.chat.id):
        await message.answer(t("admin_only", lang))
        return
    args = (command.args or "").strip().split(maxsplit=1)
    login = args[0] if args else ""
    if not login:
        await message.answer(t("unadmin_usage", lang))
        return
    own_login = db_get_user_login(message.chat.id)
    if own_login and login.lower() == own_login.lower():
        await message.answer(t("unadmin_self", lang))
        return
    try:
        result = db_set_admin_flag(login, False)
    except Exception:
        log.exception("unadmin failed")
        await message.answer(t("internal_error", lang))
        return
    if result == "not_found":
        await message.answer(t("deluser_not_found", lang, login=login), parse_mode=HTML_PARSE)
    elif result == "last_admin":
        await message.answer(t("unadmin_last", lang))
    else:
        log.info("Removed admin: %s by chat=%s", login, message.chat.id)
        await message.answer(t("unadmin_done", lang, login=login), parse_mode=HTML_PARSE)

# ============================================================
# PANEL HELPERS (used by buttons and commands)
# ============================================================
async def show_route_panel(chat_id: int, lang: str):
    routes = db_list_auto_routes(chat_id)
    filters = db_get_filters(chat_id)
    notify_on = bool(filters["notifications_enabled"])
    if routes:
        text = t("routes_list_header", lang, count=len(routes))
    else:
        text = t("routes_list_empty", lang)
    await bot.send_message(chat_id, text, parse_mode=HTML_PARSE,
                           reply_markup=routes_panel_keyboard(lang, routes, notify_on))

async def show_filters_panel(chat_id: int, lang: str):
    filters = db_get_filters(chat_id)
    view = format_filters_view(filters, lang)
    text = view if view else t("filters_none", lang)
    await bot.send_message(chat_id, text, parse_mode=HTML_PARSE,
                           reply_markup=filters_panel_keyboard(lang))

async def show_user_stats(chat_id: int, lang: str):
    stats = db_get_user_stats(chat_id)
    if stats["from_city"] and stats["to_city"]:
        route_text = f"{stats['from_city']} ➜ {stats['to_city']}"
    else:
        route_text = t("stats_no_route", lang)
    filters = db_get_filters(chat_id)
    notify_text = t("notify_status_on", lang) if filters["notifications_enabled"] else t("notify_status_off", lang)
    await bot.send_message(
        chat_id,
        t("stats_user", lang,
          searches=stats["searches"], cargos=stats["cargos"],
          route=route_text, notify=notify_text, language=stats["language"]),
        parse_mode=HTML_PARSE,
    )

async def show_settings_panel(chat_id: int, lang: str):
    await bot.send_message(chat_id, t("settings_panel", lang),
                           parse_mode=HTML_PARSE,
                           reply_markup=settings_panel_keyboard(lang))

async def show_help_text(chat_id: int, lang: str):
    text = t("help_user", lang)
    if db_is_admin(chat_id):
        text += t("help_admin", lang)
    await bot.send_message(chat_id, text, parse_mode=HTML_PARSE)

async def show_admin_panel(chat_id: int, lang: str):
    if not db_is_admin(chat_id):
        await bot.send_message(chat_id, t("admin_only", lang))
        return
    pending = db_count_pending()
    unread = db_count_unread_admin()
    await bot.send_message(chat_id, t("admin_panel", lang),
                           parse_mode=HTML_PARSE,
                           reply_markup=admin_panel_keyboard(lang, pending, unread))

async def do_route_search(chat_id: int, lang: str, text: str):
    parsed = parse_user_route(text)
    if not parsed:
        await bot.send_message(chat_id, t("route_format", lang))
        return
    from_city, to_city = parsed
    try:
        db_set_route(chat_id, from_city, to_city)
        db_add_auto_route(chat_id, from_city, to_city)  # auto-subscribe for live notifications
        db_increment_searches(chat_id)
    except Exception:
        log.exception("Failed to save route / stats")

    filters = db_get_filters(chat_id)
    has_filters = (
        filters["weight_min"] is not None or filters["weight_max"] is not None or
        filters["truck_type"] or filters["require_phone"] or
        filters["keywords_include"] or filters["keywords_exclude"]
    )

    await bot.send_message(chat_id, t("searching", lang, from_city=from_city, to_city=to_city))

    sent_hashes = set()  # de-dupe within this single search call
    sent_count = 0

    async def on_cargo(cargo):
        nonlocal sent_count
        h = cargo["hash"]
        if h in sent_hashes:
            return
        sent_hashes.add(h)
        if not cargo_passes_filters(cargo["block"], cargo.get("phone"), filters):
            return
        try:
            if db_already_sent(chat_id, h):
                return
        except Exception:
            return
        try:
            await bot.send_message(chat_id, format_cargo(cargo, is_new=False, lang=lang))
            db_mark_sent(chat_id, h)
            sent_count += 1
        except Exception:
            log.exception("Failed to send cargo to %s", chat_id)

    try:
        await search_cargos(from_city, to_city, on_cargo=on_cargo)
    except Exception:
        log.exception("Search failed chat=%s %s->%s", chat_id, from_city, to_city)
        await bot.send_message(chat_id, t("search_error", lang))
        return

    if sent_count == 0:
        key = "not_found_with_filters" if has_filters else "not_found"
        await bot.send_message(chat_id, t(key, lang, from_city=from_city, to_city=to_city))
    else:
        try:
            db_increment_cargos_received(chat_id, sent_count)
        except Exception:
            log.exception("stats increment failed")
        await bot.send_message(chat_id, t("all_for_today", lang))

# ============================================================
# HANDLERS: FSM forms (button-driven inputs)
# ============================================================
@dp.message(FormStates.setting_route)
async def form_set_route(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    text = (message.text or "").strip()
    if text.startswith("/"):
        await state.clear()
        await message.answer(t("form_cancelled", lang))
        return
    await state.clear()
    await do_route_search(chat_id, lang, text)

@dp.message(FormStates.adding_auto_route)
async def form_add_auto_route(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    text = (message.text or "").strip()
    if text.startswith("/"):
        await state.clear()
        await message.answer(t("form_cancelled", lang))
        return
    parsed = parse_user_route(text)
    if not parsed:
        await message.answer(t("route_invalid_format", lang), parse_mode=HTML_PARSE)
        return
    from_city, to_city = parsed
    try:
        added = db_add_auto_route(chat_id, from_city, to_city)
    except Exception:
        log.exception("add_auto_route failed")
        await state.clear()
        await message.answer(t("internal_error", lang))
        return
    await state.clear()
    if added:
        await message.answer(
            t("route_added", lang, from_city=from_city, to_city=to_city),
            parse_mode=HTML_PARSE,
        )
    else:
        await message.answer(t("route_already_exists", lang))
    await show_route_panel(chat_id, lang)

@dp.message(FormStates.setting_weight)
async def form_set_weight(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    value = (message.text or "").strip()
    parsed = parse_weight_filter_value(value)
    if parsed is None:
        await message.answer(t("filter_weight_usage", lang), parse_mode=HTML_PARSE)
        return
    if parsed[0] == "clear":
        db_update_filter(chat_id, weight_min=None, weight_max=None)
        await message.answer(t("filter_cleared", lang))
    elif parsed[0] == "min":
        db_update_filter(chat_id, weight_min=parsed[1])
        await message.answer(t("filter_updated", lang))
    elif parsed[0] == "max":
        db_update_filter(chat_id, weight_max=parsed[1])
        await message.answer(t("filter_updated", lang))
    elif parsed[0] == "range":
        db_update_filter(chat_id, weight_min=parsed[1], weight_max=parsed[2])
        await message.answer(t("filter_updated", lang))
    search_cache.invalidate_all()
    await state.clear()
    await show_filters_panel(chat_id, lang)

@dp.message(FormStates.setting_truck)
async def form_set_truck(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    value = (message.text or "").strip()
    if value.lower() == "clear" or not value:
        db_update_filter(chat_id, truck_type=None)
        await message.answer(t("filter_cleared", lang))
    else:
        db_update_filter(chat_id, truck_type=value)
        await message.answer(t("filter_updated", lang))
    search_cache.invalidate_all()
    await state.clear()
    await show_filters_panel(chat_id, lang)

@dp.message(FormStates.setting_include)
async def form_set_include(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    value = (message.text or "").strip()
    if value.lower() == "clear" or not value:
        db_update_filter(chat_id, keywords_include="")
        await message.answer(t("filter_cleared", lang))
    else:
        db_update_filter(chat_id, keywords_include=value)
        await message.answer(t("filter_updated", lang))
    search_cache.invalidate_all()
    await state.clear()
    await show_filters_panel(chat_id, lang)

@dp.message(FormStates.setting_exclude)
async def form_set_exclude(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    value = (message.text or "").strip()
    if value.lower() == "clear" or not value:
        db_update_filter(chat_id, keywords_exclude="")
        await message.answer(t("filter_cleared", lang))
    else:
        db_update_filter(chat_id, keywords_exclude=value)
        await message.answer(t("filter_updated", lang))
    search_cache.invalidate_all()
    await state.clear()
    await show_filters_panel(chat_id, lang)

@dp.message(FormStates.changing_password)
async def form_change_password(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    new_pass = (message.text or "").strip()
    if len(new_pass) < MIN_PASSWORD_LENGTH:
        await message.answer(t("changepass_short", lang))
        return
    own_login = db_get_user_login(chat_id)
    if not own_login:
        await state.clear()
        await message.answer(t("not_authorized", lang))
        return
    db_set_password(own_login, new_pass)
    await state.clear()
    await message.answer(t("changepass_done", lang))

@dp.message(FormStates.add_user_login)
async def form_add_user_login(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    login = (message.text or "").strip()
    if not LOGIN_RE.match(login):
        await message.answer(t("adduser_invalid_login", lang, login=login), parse_mode=HTML_PARSE)
        return
    if db_login_exists(login):
        await message.answer(t("adduser_already_exists", lang, login=login), parse_mode=HTML_PARSE)
        return
    await state.update_data(login=login)
    await state.set_state(FormStates.add_user_password)
    await message.answer(t("form_enter_password", lang))

@dp.message(FormStates.add_user_password)
async def form_add_user_password(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    password = (message.text or "").strip()
    if len(password) < MIN_PASSWORD_LENGTH:
        await message.answer(t("adduser_password_short", lang))
        return
    await state.update_data(password=password)
    await state.set_state(FormStates.add_user_admin_choice)
    await message.answer(
        t("form_confirm_admin", lang),
        reply_markup=yes_no_keyboard(lang, "cb:add_user_yes", "cb:add_user_no"),
    )

@dp.message(FormStates.reset_pass_for_login)
async def form_reset_password(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    new_pass = (message.text or "").strip()
    if len(new_pass) < MIN_PASSWORD_LENGTH:
        await message.answer(t("adduser_password_short", lang))
        return
    data = await state.get_data()
    target_login = data.get("login", "")
    if not db_login_exists(target_login):
        await state.clear()
        await message.answer(t("deluser_not_found", lang, login=target_login), parse_mode=HTML_PARSE)
        return
    db_set_password(target_login, new_pass, store_plaintext=True)
    await state.clear()
    await message.answer(
        t("resetpass_done", lang, login=target_login, password=new_pass),
        parse_mode=HTML_PARSE,
    )

@dp.message(FormStates.add_group_input)
async def form_add_group(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    args = (message.text or "").strip()
    username = normalize_group_username(args)
    if not username:
        await message.answer(t("group_invalid", lang, name=args))
        return
    if username in db_get_group_usernames():
        await message.answer(t("group_already", lang, name=f"@{username}"))
        await state.clear()
        return
    title = username  # filled in the first time the bot sees a message there
    db_add_group(username, title, chat_id)
    invalidate_groups_cache()
    search_cache.invalidate_all()
    await state.clear()
    await message.answer(t("group_added", lang, name=f"@{username} — {title}"))

def _extract_media(message: types.Message):
    """Returns (text, file_id, file_type)."""
    text = message.text or message.caption or ""
    file_id = None
    file_type = None
    if message.photo:
        file_id = message.photo[-1].file_id
        file_type = "photo"
    elif message.document:
        file_id = message.document.file_id
        file_type = "document"
    elif message.video:
        file_id = message.video.file_id
        file_type = "video"
    elif message.voice:
        file_id = message.voice.file_id
        file_type = "voice"
    return text, file_id, file_type

async def _send_message_payload(target_chat_id: int, text: str, file_id, file_type, header: str = ""):
    """Send a stored message (text + optional media) to target."""
    caption = (header + ("\n\n" + text if text else "")).strip() or None
    if file_id and file_type == "photo":
        await bot.send_photo(target_chat_id, file_id, caption=caption, parse_mode=HTML_PARSE)
    elif file_id and file_type == "document":
        await bot.send_document(target_chat_id, file_id, caption=caption, parse_mode=HTML_PARSE)
    elif file_id and file_type == "video":
        await bot.send_video(target_chat_id, file_id, caption=caption, parse_mode=HTML_PARSE)
    elif file_id and file_type == "voice":
        if header:
            await bot.send_message(target_chat_id, header, parse_mode=HTML_PARSE)
        await bot.send_voice(target_chat_id, file_id, caption=text or None)
    else:
        body = caption or text or ""
        if body:
            await bot.send_message(target_chat_id, body, parse_mode=HTML_PARSE)

@dp.message(FormStates.messaging_admin)
async def form_message_admin(message: types.Message, state: FSMContext):
    """User sends a message to admin (text/photo/document/video/voice)."""
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    text, file_id, file_type = _extract_media(message)
    if not text and not file_id:
        await state.clear()
        await message.answer(t("form_cancelled", lang))
        return

    sender_login = db_get_user_login(chat_id) or ""
    db_save_message(chat_id, "in", chat_id, sender_login, text, file_id, file_type)

    # Notify all admins immediately
    name = _user_display_name(message.from_user)
    phone = db_get_phone(chat_id) or "-"
    preview = text[:100] if text else f"[{file_type or 'attachment'}]"
    for admin_chat in db_admin_chat_ids():
        try:
            admin_lang = db_get_language(admin_chat)
            await bot.send_message(
                admin_chat,
                t("admin_new_message_notification", admin_lang,
                  name=name, phone=phone, preview=preview),
                parse_mode=HTML_PARSE,
            )
            # Also forward the actual content (so admins see media inline)
            await _send_message_payload(
                admin_chat, text, file_id, file_type,
                header=t("msg_user_label", admin_lang) + f": {name}",
            )
        except Exception:
            log.exception("Failed to notify admin %s about new message from %s", admin_chat, chat_id)

    # If unauthorized, return to access_choice so user can send another message or login.
    if not db_is_authorized(chat_id):
        await state.set_state(AuthStates.access_choice)
        await message.answer(
            t("waiting_for_admin_reply", lang),
            reply_markup=access_request_keyboard(lang),
        )
    else:
        await state.clear()
        await message.answer(t("message_sent_to_admin", lang))

@dp.message(FormStates.replying_to_user)
async def form_reply_to_user(message: types.Message, state: FSMContext):
    """Admin replies to a user (text/photo/document/video/voice)."""
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    data = await state.get_data()
    target_user = data.get("reply_target")
    if not target_user:
        await state.clear()
        await message.answer(t("form_cancelled", lang))
        return

    text, file_id, file_type = _extract_media(message)
    if not text and not file_id:
        await state.clear()
        await message.answer(t("form_cancelled", lang))
        return

    sender_login = db_get_user_login(chat_id) or ""
    db_save_message(target_user, "out", chat_id, sender_login, text, file_id, file_type)

    # Send to user
    target_lang = db_get_language(target_user)
    try:
        await _send_message_payload(
            target_user, text, file_id, file_type,
            header=t("user_received_admin_reply", target_lang),
        )
    except Exception:
        log.exception("Failed to deliver admin reply to %s", target_user)
        await message.answer(t("internal_error", lang))
        await state.clear()
        return

    await state.clear()
    await message.answer(t("admin_reply_sent", lang))

@dp.message(FormStates.broadcast_input)
async def form_broadcast(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    text = (message.text or "").strip()
    if not text:
        await state.clear()
        await message.answer(t("form_cancelled", lang))
        return
    data = await state.get_data()
    recipients = data.get("bcast_recipients", "all")
    if recipients == "all":
        chat_ids = db_get_authorized_chat_ids()
    else:  # list of logins
        chat_ids = db_chat_ids_by_logins(recipients)

    sent = failed = 0
    for cid in chat_ids:
        try:
            recipient_lang = db_get_language(cid)
            await bot.send_message(
                cid,
                t("broadcast_received", recipient_lang, message=text),
                parse_mode=HTML_PARSE,
            )
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(BROADCAST_RATE_DELAY)
    await state.clear()
    await message.answer(t("broadcast_done", lang, sent=sent, failed=failed))

# ============================================================
# HANDLERS: callback queries (inline buttons)
# ============================================================
@dp.callback_query(F.data.startswith("cb:"))
async def handle_callback(query: CallbackQuery, state: FSMContext):
    try:
        await _route_callback(query, state)
    except Exception:
        log.exception("Callback handler error: %s", query.data)
    finally:
        try:
            await query.answer()
        except Exception:
            pass

async def _route_callback(query: CallbackQuery, state: FSMContext):
    chat_id = query.from_user.id
    lang = db_get_language(chat_id)
    if not db_is_authorized(chat_id):
        await query.answer(t("not_authorized", lang), show_alert=True)
        return

    parts = query.data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    args = parts[2:]

    # Cancel any form
    if action == "cancel":
        await state.clear()
        await query.answer(t("form_cancelled", lang))
        return

    # ---- Route panel ----
    if action == "route_change":
        await state.set_state(FormStates.setting_route)
        await bot.send_message(chat_id, t("form_enter_route", lang),
                               parse_mode=HTML_PARSE, reply_markup=cancel_keyboard(lang))
        return
    if action == "route_clear":
        db_clear_route(chat_id)
        await query.answer(t("clearroute_done", lang))
        await show_route_panel(chat_id, lang)
        return
    if action == "route_notify_toggle":
        cur = db_get_filters(chat_id)
        new_val = 0 if cur["notifications_enabled"] else 1
        db_update_filter(chat_id, notifications_enabled=new_val)
        await query.answer(t("notify_on" if new_val else "notify_off", lang))
        await show_route_panel(chat_id, lang)
        return
    if action == "autoroute_add":
        await state.set_state(FormStates.adding_auto_route)
        await bot.send_message(chat_id, t("form_enter_new_route", lang),
                               parse_mode=HTML_PARSE, reply_markup=cancel_keyboard(lang))
        return
    if action == "autoroute_del":
        rid = int(args[0]) if args else 0
        if rid and db_delete_auto_route(rid, chat_id):
            await query.answer(t("route_deleted", lang))
        await show_route_panel(chat_id, lang)
        return

    # ---- Filter panel ----
    if action == "filter_weight":
        await state.set_state(FormStates.setting_weight)
        await bot.send_message(chat_id, t("form_enter_weight", lang),
                               parse_mode=HTML_PARSE, reply_markup=cancel_keyboard(lang))
        return
    if action == "filter_truck":
        await state.set_state(FormStates.setting_truck)
        await bot.send_message(chat_id, t("form_enter_truck", lang),
                               parse_mode=HTML_PARSE, reply_markup=cancel_keyboard(lang))
        return
    if action == "filter_phone":
        cur = db_get_filters(chat_id)
        new_val = 0 if cur["require_phone"] else 1
        db_update_filter(chat_id, require_phone=new_val)
        search_cache.invalidate_all()
        await query.answer(t("filter_updated", lang))
        await show_filters_panel(chat_id, lang)
        return
    if action == "filter_include":
        await state.set_state(FormStates.setting_include)
        await bot.send_message(chat_id, t("form_enter_include", lang),
                               parse_mode=HTML_PARSE, reply_markup=cancel_keyboard(lang))
        return
    if action == "filter_exclude":
        await state.set_state(FormStates.setting_exclude)
        await bot.send_message(chat_id, t("form_enter_exclude", lang),
                               parse_mode=HTML_PARSE, reply_markup=cancel_keyboard(lang))
        return
    if action == "filter_clear_all":
        db_clear_all_filters(chat_id)
        search_cache.invalidate_all()
        await query.answer(t("clearfilters_done", lang))
        await show_filters_panel(chat_id, lang)
        return

    # ---- Settings panel ----
    if action == "set_lang":
        await state.set_state(AuthStates.changing_language)
        await bot.send_message(chat_id, t("lang_prompt", lang),
                               reply_markup=language_keyboard())
        return
    if action == "change_pass":
        await state.set_state(FormStates.changing_password)
        await bot.send_message(chat_id, t("form_enter_new_password", lang),
                               reply_markup=cancel_keyboard(lang))
        return
    if action == "logout":
        db_set_unauthorized(chat_id)
        await state.clear()
        await bot.send_message(chat_id, t("logout", lang),
                               reply_markup=ReplyKeyboardRemove())
        return

    # ---- Admin panel ----
    if not db_is_admin(chat_id):
        await query.answer(t("admin_only", lang), show_alert=True)
        return

    if action == "admin_back":
        await show_admin_panel(chat_id, lang)
        return
    if action == "admin_users":
        own_login = db_get_user_login(chat_id) or ""
        accounts = db_list_accounts()
        await bot.send_message(
            chat_id,
            t("accounts_header", lang, count=len(accounts)),
            parse_mode=HTML_PARSE,
            reply_markup=admin_users_keyboard(lang, accounts, own_login),
        )
        return
    if action == "admin_groups":
        groups = db_list_groups()
        text = t("groups_header", lang, count=len(groups)) if groups else t("groups_empty", lang)
        await bot.send_message(chat_id, text,
                               reply_markup=admin_groups_keyboard(lang, groups))
        return
    if action == "admin_stats":
        stats = db_get_admin_stats()
        await bot.send_message(chat_id, t("admin_users", lang, **stats),
                               parse_mode=HTML_PARSE)
        return
    if action == "admin_broadcast":
        await bot.send_message(chat_id, t("bcast_choose_target", lang),
                               reply_markup=broadcast_target_keyboard(lang))
        return
    if action == "bcast_all":
        await state.update_data(bcast_recipients="all")
        await state.set_state(FormStates.broadcast_input)
        await bot.send_message(chat_id, t("form_enter_broadcast", lang),
                               reply_markup=cancel_keyboard(lang))
        return
    if action == "bcast_select":
        accounts = db_list_accounts()
        eligible = [a for a in accounts if a["sessions"] > 0]
        if not eligible:
            await bot.send_message(chat_id, t("bcast_no_users", lang))
            return
        await state.update_data(bcast_selected=[])
        await bot.send_message(
            chat_id,
            t("bcast_select_users", lang, count=0),
            reply_markup=broadcast_users_keyboard(lang, accounts, set()),
        )
        return
    if action == "bcast_toggle":
        login = args[0] if args else ""
        data = await state.get_data()
        selected = set(data.get("bcast_selected", []))
        if login in selected:
            selected.discard(login)
        else:
            selected.add(login)
        await state.update_data(bcast_selected=list(selected))
        accounts = db_list_accounts()
        try:
            await query.message.edit_reply_markup(
                reply_markup=broadcast_users_keyboard(lang, accounts, selected),
            )
            await query.message.edit_text(
                t("bcast_select_users", lang, count=len(selected)),
                reply_markup=broadcast_users_keyboard(lang, accounts, selected),
            )
        except Exception:
            # Some clients won't allow edit_text after edit_reply_markup; ignore.
            pass
        return
    if action == "bcast_pick_all":
        accounts = db_list_accounts()
        all_logins = [a["login"] for a in accounts if a["sessions"] > 0]
        await state.update_data(bcast_selected=all_logins)
        try:
            await query.message.edit_text(
                t("bcast_select_users", lang, count=len(all_logins)),
                reply_markup=broadcast_users_keyboard(lang, accounts, set(all_logins)),
            )
        except Exception:
            pass
        return
    if action == "bcast_clear_sel":
        await state.update_data(bcast_selected=[])
        accounts = db_list_accounts()
        try:
            await query.message.edit_text(
                t("bcast_select_users", lang, count=0),
                reply_markup=broadcast_users_keyboard(lang, accounts, set()),
            )
        except Exception:
            pass
        return
    if action == "bcast_send_selected":
        data = await state.get_data()
        selected = list(data.get("bcast_selected", []))
        if not selected:
            await query.answer(t("bcast_no_selected", lang), show_alert=True)
            return
        await state.update_data(bcast_recipients=selected)
        await state.set_state(FormStates.broadcast_input)
        await bot.send_message(chat_id, t("form_enter_broadcast", lang),
                               reply_markup=cancel_keyboard(lang))
        return
    if action == "admin_messages":
        convs = db_list_conversations()
        if not convs:
            await bot.send_message(chat_id, t("admin_messages_empty", lang))
            return
        # Resolve names
        items = []
        unread_total = 0
        for c in convs:
            unread_total += c["unread"]
            try:
                tg_chat = await bot.get_chat(c["user_chat_id"])
                name = tg_chat.username and f"@{tg_chat.username}" or tg_chat.full_name or f"id{c['user_chat_id']}"
            except Exception:
                name = f"id{c['user_chat_id']}"
            items.append({**c, "name": name})
        await bot.send_message(
            chat_id,
            t("admin_messages_header", lang, count=len(items), unread=unread_total),
            parse_mode=HTML_PARSE,
            reply_markup=admin_messages_keyboard(lang, items),
        )
        return
    if action == "msg_open":
        target = int(args[0]) if args else 0
        if not target:
            return
        # Mark as read
        db_mark_conversation_read(target)
        # Build header
        prof = db_get_profile(target)
        try:
            tg_chat = await bot.get_chat(target)
            name = tg_chat.full_name or "?"
            tg_username = tg_chat.username and f"@{tg_chat.username}" or "-"
        except Exception:
            name = prof.get("first_name") or "?"
            tg_username = prof.get("telegram_username") or "-"
            if tg_username and tg_username != "-":
                tg_username = f"@{tg_username}"
        phone = prof.get("phone") or "-"
        await bot.send_message(
            chat_id,
            t("admin_msg_thread_user_info", lang,
              name=name, username=tg_username, phone=phone, chat_id=target),
            parse_mode=HTML_PARSE,
        )
        # Replay messages chronologically
        msgs = db_get_conversation(target)
        for m in msgs:
            if m["direction"] == "in":
                header = t("msg_user_label", lang)
            else:
                header = t("msg_admin_label", lang, login=m.get("sender_login") or "?")
            try:
                await _send_message_payload(
                    chat_id, m["text"] or "", m["file_id"], m["file_type"],
                    header=header,
                )
            except Exception:
                log.exception("Failed to replay message %s", m["id"])
        # Reply button
        await bot.send_message(
            chat_id,
            t("admin_msg_thread_header", lang, name=name),
            parse_mode=HTML_PARSE,
            reply_markup=admin_msg_thread_keyboard(lang, target),
        )
        return
    if action == "msg_reply":
        target = int(args[0]) if args else 0
        if not target:
            return
        await state.update_data(reply_target=target)
        await state.set_state(FormStates.replying_to_user)
        await bot.send_message(chat_id, t("form_enter_reply_to_user", lang),
                               reply_markup=cancel_keyboard(lang))
        return

    if action == "admin_pending":
        pending = db_list_pending()
        if not pending:
            await bot.send_message(chat_id, t("admin_pending_empty", lang))
            return
        items = []
        for p in pending[:20]:
            prof = db_get_profile(p["chat_id"])
            try:
                tg_chat = await bot.get_chat(p["chat_id"])
                name = tg_chat.full_name or prof.get("first_name") or f"id{p['chat_id']}"
                if tg_chat.username:
                    name = f"{name} (@{tg_chat.username})"
            except Exception:
                name = prof.get("first_name") or f"id{p['chat_id']}"
            phone = prof.get("phone") or "-"
            label = name + (f" 📞 {phone}" if phone != "-" else "")
            items.append({"chat_id": p["chat_id"], "name": label, "started": p["started"]})
        await bot.send_message(
            chat_id,
            t("admin_pending_header", lang, count=len(items)),
            parse_mode=HTML_PARSE,
            reply_markup=admin_pending_keyboard(lang, items),
        )
        return

    if action == "add_user":
        await state.set_state(FormStates.add_user_login)
        await bot.send_message(chat_id, t("form_enter_login", lang),
                               reply_markup=cancel_keyboard(lang))
        return
    if action == "add_user_yes" or action == "add_user_no":
        data = await state.get_data()
        login = data.get("login")
        password = data.get("password")
        if not (login and password):
            await state.clear()
            return
        is_admin_flag = (action == "add_user_yes")
        try:
            ok = db_create_account(login, password, is_admin_flag)
        except Exception:
            log.exception("create account from form failed")
            await state.clear()
            await bot.send_message(chat_id, t("internal_error", lang))
            return
        await state.clear()
        if not ok:
            await bot.send_message(chat_id,
                t("adduser_already_exists", lang, login=login),
                parse_mode=HTML_PARSE)
            return
        await bot.send_message(
            chat_id,
            t("adduser_done", lang, login=login, password=password,
              admin=t("filter_yes" if is_admin_flag else "filter_no", lang)),
            parse_mode=HTML_PARSE,
        )
        return
    if action == "user":  # show single user actions
        login = args[0] if args else ""
        accounts = {a["login"]: a for a in db_list_accounts()}
        acc = accounts.get(login)
        if not acc:
            await bot.send_message(chat_id, t("deluser_not_found", lang, login=login),
                                   parse_mode=HTML_PARSE)
            return
        own_login = db_get_user_login(chat_id) or ""
        is_self = (login.lower() == own_login.lower())
        last_pw = db_get_last_password(login) or "—"
        await bot.send_message(
            chat_id,
            t("admin_user_actions", lang,
              login=login,
              is_admin=t("filter_yes" if acc["is_admin"] else "filter_no", lang),
              password=last_pw,
              sessions=acc["sessions"], routes=acc["routes"]),
            parse_mode=HTML_PARSE,
            reply_markup=admin_user_actions_keyboard(lang, login, acc["is_admin"], is_self),
        )
        return
    if action == "user_reset":
        login = args[0] if args else ""
        if not db_login_exists(login):
            await bot.send_message(chat_id, t("deluser_not_found", lang, login=login),
                                   parse_mode=HTML_PARSE)
            return
        await state.update_data(login=login)
        await state.set_state(FormStates.reset_pass_for_login)
        await bot.send_message(chat_id, t("form_enter_new_password", lang),
                               reply_markup=cancel_keyboard(lang))
        return
    if action == "user_makeadmin":
        login = args[0] if args else ""
        result = db_set_admin_flag(login, True)
        if result == "not_found":
            await bot.send_message(chat_id, t("deluser_not_found", lang, login=login), parse_mode=HTML_PARSE)
        else:
            await bot.send_message(chat_id, t("makeadmin_done", lang, login=login), parse_mode=HTML_PARSE)
        return
    if action == "user_unadmin":
        login = args[0] if args else ""
        own_login = db_get_user_login(chat_id) or ""
        if login.lower() == own_login.lower():
            await bot.send_message(chat_id, t("unadmin_self", lang))
            return
        result = db_set_admin_flag(login, False)
        if result == "not_found":
            await bot.send_message(chat_id, t("deluser_not_found", lang, login=login), parse_mode=HTML_PARSE)
        elif result == "last_admin":
            await bot.send_message(chat_id, t("unadmin_last", lang))
        else:
            await bot.send_message(chat_id, t("unadmin_done", lang, login=login), parse_mode=HTML_PARSE)
        return
    if action == "user_delete":
        login = args[0] if args else ""
        own_login = db_get_user_login(chat_id) or ""
        if login.lower() == own_login.lower():
            await bot.send_message(chat_id, t("deluser_self", lang))
            return
        result = db_delete_account(login)
        if result == "not_found":
            await bot.send_message(chat_id, t("deluser_not_found", lang, login=login), parse_mode=HTML_PARSE)
        elif result == "last_admin":
            await bot.send_message(chat_id, t("deluser_last_admin", lang))
        else:
            await bot.send_message(chat_id, t("deluser_done", lang, login=login), parse_mode=HTML_PARSE)
        return

    if action == "add_group":
        await state.set_state(FormStates.add_group_input)
        await bot.send_message(chat_id, t("form_enter_group", lang),
                               parse_mode=HTML_PARSE, reply_markup=cancel_keyboard(lang))
        return
    if action == "group":
        username = args[0] if args else ""
        title = db_get_group_title(username) or username
        await bot.send_message(
            chat_id,
            t("admin_group_actions", lang, username=username, title=title, added_at="-"),
            parse_mode=HTML_PARSE,
            reply_markup=admin_group_actions_keyboard(lang, username),
        )
        return
    if action == "group_delete":
        username = args[0] if args else ""
        if db_remove_group(username):
            invalidate_groups_cache()
            search_cache.invalidate_all()
            await bot.send_message(chat_id, t("group_removed", lang, name=f"@{username}"))
        else:
            await bot.send_message(chat_id, t("group_not_found", lang, name=f"@{username}"))
        return

    if action == "pending":
        pending_chat_id = int(args[0]) if args else 0
        prof = db_get_profile(pending_chat_id)
        try:
            tg_chat = await bot.get_chat(pending_chat_id)
            name = tg_chat.full_name or prof.get("first_name") or f"id{pending_chat_id}"
            if tg_chat.username:
                name = f"{name} (@{tg_chat.username})"
        except Exception:
            name = prof.get("first_name") or f"id{pending_chat_id}"
        phone = prof.get("phone") or "-"
        info_text = (
            t("admin_pending_item", lang, name=name, chat_id=pending_chat_id, started="-")
            + f"\n📞 {phone}"
        )
        await bot.send_message(
            chat_id, info_text, parse_mode=HTML_PARSE,
            reply_markup=admin_pending_actions_keyboard(lang, pending_chat_id),
        )
        return
    if action == "approve":
        target = int(args[0]) if args else 0
        if not target:
            return
        try:
            chat = await bot.get_chat(target)
            base_hint = chat.username or (chat.first_name or "")
        except Exception:
            base_hint = ""
        new_login = generate_login(base_hint)
        new_password = generate_password()
        db_create_account(new_login, new_password, is_admin=False)
        # Link login to user's existing row (preserves language/phone) — they're no longer pending
        db_link_login_to_chat(target, new_login)
        # Send credentials to user
        try:
            target_lang = db_get_language(target)
            await bot.send_message(
                target,
                t("user_received_credentials", target_lang,
                  login=new_login, password=new_password),
                parse_mode=HTML_PARSE,
            )
            await bot.send_message(
                chat_id,
                t("credentials_sent_to_user", lang, login=new_login),
                parse_mode=HTML_PARSE,
            )
        except Exception:
            log.exception("Failed to DM new user %s", target)
            await bot.send_message(chat_id, t("internal_error", lang))
        return
    if action == "reject":
        target = int(args[0]) if args else 0
        if not target:
            return
        db_remove_pending(target)
        try:
            target_lang = db_get_language(target)
            await bot.send_message(target, t("pending_user_was_rejected", target_lang))
        except Exception:
            pass
        await bot.send_message(chat_id, t("pending_rejected", lang))
        return

# ============================================================
# HANDLERS: catch-all (button text or route search)
# ============================================================
@dp.message()
async def catch_all(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    lang = db_get_language(chat_id)
    try:
        if not db_is_authorized(chat_id):
            await message.answer(t("not_authorized", lang))
            return
    except Exception:
        log.exception("Authorization check failed")
        await message.answer(t("internal_error", lang))
        return

    text = (message.text or "").strip()

    # Detect main menu button
    action = detect_main_button(text)
    if action == "search":
        await message.answer(t("search_prompt", lang), parse_mode=HTML_PARSE)
        return
    if action == "route":
        await show_route_panel(chat_id, lang)
        return
    if action == "filters":
        await show_filters_panel(chat_id, lang)
        return
    if action == "stats":
        await show_user_stats(chat_id, lang)
        return
    if action == "settings":
        await show_settings_panel(chat_id, lang)
        return
    if action == "help":
        await show_help_text(chat_id, lang)
        return
    if action == "admin":
        await show_admin_panel(chat_id, lang)
        return
    if action == "msg_admin":
        if not db_admin_chat_ids():
            await message.answer(t("message_no_admins", lang))
            return
        await state.set_state(FormStates.messaging_admin)
        await bot.send_message(chat_id, t("form_enter_message_to_admin", lang),
                               reply_markup=cancel_keyboard(lang))
        return

    # Otherwise treat as route search
    await do_route_search(chat_id, lang, text)

# ============================================================
# SEARCH — local SQLite store (bot-only mode, no Telethon required)
# ============================================================
async def search_cargos(from_city: str, to_city: str, on_cargo=None):
    """Scan locally-stored group messages for cargos matching the route.
    If on_cargo is provided, results are streamed via the callback."""
    cache_key = (normalize_text(from_city), normalize_text(to_city))
    cached = search_cache.get(cache_key)
    if cached is not None:
        log.info("Cache hit %s -> %s (%d)", from_city, to_city, len(cached))
        if on_cargo:
            for c in cached:
                try:
                    await on_cargo(c)
                except Exception:
                    log.exception("on_cargo callback failed (cache)")
        return cached

    cutoff = datetime.now(UZ_TIME) - timedelta(hours=SEARCH_LOOKBACK_HOURS)
    monitored_groups = get_group_usernames_cached()
    if not monitored_groups:
        return []

    started = time.monotonic()
    rows = await asyncio.to_thread(db_iter_recent_messages, cutoff)

    results = []
    for row in rows:
        group_username = row["group_username"]
        if group_username not in monitored_groups:
            continue
        text = row["text"] or ""
        if not text:
            continue
        title = row["group_title"] or group_username
        try:
            posted_dt = datetime.strptime(row["posted_at"], "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            posted_dt = datetime.now(UZ_TIME)
        msg_time = posted_dt.strftime("%H:%M")

        blocks = split_blocks(text)
        if not blocks:
            continue
        for block in blocks:
            if not match_route(block, from_city, to_city):
                continue
            phone = extract_phone(block) or extract_phone(text)
            sender_name = row["sender_name"] or row["sender_username"]
            cargo = {
                "hash": cargo_hash(block),
                "block": block,
                "phone": phone,
                "sender": sender_name,
                "source": title,
                "time": msg_time,
            }
            results.append(cargo)
            if on_cargo:
                try:
                    await on_cargo(cargo)
                except Exception:
                    log.exception("on_cargo callback failed")

    elapsed = time.monotonic() - started

    if results:
        search_cache.set(cache_key, results)
    log.info("Searched %s -> %s in %.2fs, %d cargos (local DB, %d groups)",
             from_city, to_city, elapsed, len(results), len(monitored_groups))
    return results

def format_cargo(cargo: dict, is_new: bool, lang: str) -> str:
    title = t("new_cargo", lang) if is_new else t("cargo_found", lang)
    sender_name = format_username(cargo.get("sender"), lang)
    phone_text = f"\n{t('phone_label', lang)}: {cargo['phone']}" if cargo.get("phone") else ""
    return (
        f"{title}\n"
        f"🕒 {cargo['time']}\n"
        f"{t('sender_label', lang)}: {sender_name}\n"
        f"{t('source_label', lang)}: {cargo['source']}\n\n"
        f"{cargo['block']}{phone_text}"
    )

# ============================================================
# LIVE GROUP HANDLER (aiogram bot, no Telethon needed)
# ============================================================
# Bot must be a member of cargo groups with privacy mode DISABLED in
# @BotFather (/mybots → bot → Bot Settings → Group Privacy → Turn off)
# so it sees every message, not just commands.
@dp.message(F.chat.type.in_({"group", "supergroup", "channel"}))
async def handle_group_message(message: types.Message):
    try:
        text = message.text or message.caption or ""
        if not text:
            return

        chat = message.chat
        chat_username = getattr(chat, "username", None)
        if not chat_username:
            return
        chat_username_lc = chat_username.lower()
        if chat_username_lc not in get_group_usernames_cached():
            return

        title = db_get_group_title(chat_username_lc) or (chat.title or chat_username)
        # First time we see this group's real title — persist it
        if chat.title and (title == chat_username_lc or title == chat_username):
            try:
                if db_update_group_title(chat_username_lc, chat.title):
                    invalidate_groups_cache()
                    title = chat.title
            except Exception:
                log.exception("Failed to update group title")
        posted_at = message.date.astimezone(UZ_TIME) if message.date else datetime.now(UZ_TIME)

        sender = message.from_user
        sender_id = sender.id if sender else None
        sender_name = (sender.full_name if sender else None) or (chat.title if chat else None)
        sender_username = sender.username if sender else None

        # Persist for future searches
        try:
            await asyncio.to_thread(
                db_store_cargo_message,
                chat_username_lc, title, message.message_id, text, posted_at,
                sender_id, sender_name, sender_username,
            )
        except Exception:
            log.exception("Failed to persist cargo message")

        blocks = split_blocks(text)
        if not blocks:
            return

        # Fresh content arrived — drop stale search results.
        search_cache.invalidate_all()

        msg_time = posted_at.strftime("%H:%M")
        sender_display = (f"@{sender_username}" if sender_username
                          else (sender_name or ""))

        try:
            routes = get_active_routes_cached()
        except Exception:
            log.exception("Failed to read routes")
            return
        if not routes:
            return

        for chat_id, from_city, to_city, lang, filters in routes:
            for block in blocks:
                if not match_route(block, from_city, to_city):
                    continue
                phone = extract_phone(block) or extract_phone(text)
                if not cargo_passes_filters(block, phone, filters):
                    continue
                ch = cargo_hash(block)
                try:
                    if db_already_sent(chat_id, ch):
                        continue
                    cargo = {
                        "hash": ch,
                        "block": block,
                        "phone": phone,
                        "sender": sender_display,
                        "source": title,
                        "time": msg_time,
                    }
                    await bot.send_message(chat_id, format_cargo(cargo, is_new=True, lang=lang))
                    db_mark_sent(chat_id, ch)
                    db_increment_cargos_received(chat_id, 1)
                except Exception:
                    log.exception("Failed to push live cargo to %s", chat_id)
    except Exception:
        log.exception("Live message handler error")

# ============================================================
# BACKGROUND CLEANUP
# ============================================================
async def periodic_cleanup():
    while True:
        try:
            db_cleanup_sent()
            db_cleanup_cargo_messages(retention_hours=max(SEARCH_LOOKBACK_HOURS * 2, 48))
            search_cache.cleanup()
        except Exception:
            log.exception("Periodic cleanup failed")
        await asyncio.sleep(3600)

# ============================================================
# MAIN
# ============================================================
async def main():
    init_db()
    log.info(
        "Bot starting (aiogram-only mode); %d groups configured",
        len(db_get_group_usernames()),
    )
    log.info(
        "Reminder: bot must be a MEMBER of every cargo group with privacy "
        "mode DISABLED in @BotFather (Bot Settings -> Group Privacy -> Turn off)."
    )

    cleanup_task = asyncio.create_task(periodic_cleanup())
    try:
        log.info("Starting aiogram polling")
        await dp.start_polling(bot)
    finally:
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass
        try:
            await bot.session.close()
        except Exception:
            log.exception("Failed to close bot session")
        log.info("Bot stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Interrupted, exiting")
