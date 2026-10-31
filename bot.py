#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bot.py — Telegram-бот (aiogram 3) + фоновый воркер (Playwright) + SQLite (aiosqlite).

Что делает:
  * собирает у пользователя через FSM: описание, имя, email, телефон;
  * после подтверждения кладёт заявку в БД (status = pending);
  * воркер по очереди открывает форму поддержки, заполняет её "живым" вводом и отправляет;
  * при любом сбое (в т.ч. капча) делает скриншот в screenshots/ и шлёт администратору отчёт;
  * админ-команды: /cfg, /set КЛЮЧ значение, /queue, /retry ID.

Запуск:  BOT_TOKEN=123:ABC python bot.py     (подробности — в README к ответу)
"""

# ----------------------------------------------------------------------------
# 1. BOOTSTRAP: доустановка зависимостей и браузера (до импорта сторонних либ)
# ----------------------------------------------------------------------------
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

REQUIRED = {  # имя модуля -> имя пакета в pip
    "aiogram": "aiogram>=3.4",
    "aiosqlite": "aiosqlite",
    "playwright": "playwright",
}


def _chromium_installed() -> bool:
    """Грубая проверка: есть ли папка chromium-* в кэше Playwright."""
    custom = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    candidates = []
    if custom and custom != "0":
        candidates.append(Path(custom))
    candidates += [
        Path.home() / ".cache" / "ms-playwright",                  # Linux
        Path.home() / "Library" / "Caches" / "ms-playwright",      # macOS
        Path(os.environ.get("LOCALAPPDATA", "")) / "ms-playwright",  # Windows
    ]
    for base in candidates:
        if base.is_dir() and any(p.name.startswith("chromium") for p in base.iterdir()):
            return True
    return False


def bootstrap() -> None:
    """Проверяет и доустанавливает библиотеки и бинарники Chromium."""
    missing = [pkg for mod, pkg in REQUIRED.items() if importlib.util.find_spec(mod) is None]
    if missing:
        print(f"[bootstrap] Устанавливаю: {', '.join(missing)}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", *missing])
    if not _chromium_installed():
        print("[bootstrap] Устанавливаю Chromium для Playwright (может занять пару минут)...")
        cmd = [sys.executable, "-m", "playwright", "install", "chromium"]
        # На чистом Linux-сервере нужны системные библиотеки (требует root):
        if sys.platform.startswith("linux") and hasattr(os, "geteuid") and os.geteuid() == 0:
            cmd.append("--with-deps")
        subprocess.check_call(cmd)


bootstrap()

# ----------------------------------------------------------------------------
# 2. ИМПОРТЫ (после bootstrap)
# ----------------------------------------------------------------------------
import asyncio
import html
import logging
import random
import re
import time
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from playwright.async_api import Page, async_playwright

# ----------------------------------------------------------------------------
# 3. КОНСТАНТЫ И НАСТРОЙКИ ПО УМОЛЧАНИЮ
# ----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bot.db"
SHOTS_DIR = BASE_DIR / "screenshots"
SHOTS_DIR.mkdir(exist_ok=True)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()

# Все эти ключи можно менять на лету: /set КЛЮЧ значение
# ВАЖНО: селекторы ниже — разумные значения по умолчанию, но вёрстку формы я не могу
# проверить отсюда. Если поле не находится — откройте страницу в браузере, посмотрите
# атрибуты name/placeholder и поменяйте через /set (например /set SEL_NAME input[name="x"]).
DEFAULTS: dict[str, str] = {
    "ADMIN_ID": "",
    "URL": "https://telegram.org/support",
    "SEL_PROBLEM": 'textarea[name="problem"], form textarea',
    "SEL_NAME": 'input[name="legal_name"], input[name="name"], '
                'input[placeholder*="имя" i], input[placeholder*="name" i]',
    "SEL_EMAIL": 'input[name="email"], input[type="email"]',
    "SEL_PHONE": 'input[name="phone"], input[type="tel"]',
    "SEL_SUBMIT": 'button[type="submit"], input[type="submit"], form button.btn',
    # Признаки капчи (через запятую внутри одного CSS-селектора)
    "SEL_CAPTCHA": 'iframe[src*="recaptcha"], iframe[src*="hcaptcha"], '
                   'iframe[src*="challenges.cloudflare.com"], .g-recaptcha, .h-captcha, '
                   '.cf-turnstile, #cf-challenge-running, #challenge-form',
    # Тексты на странице, говорящие об успешной отправке (через запятую, без регистра)
    "SUCCESS_TEXT": "thank you,thanks,спасибо,received,получено,submitted,отправлено",
    "HEADLESS": "1",            # 1 — без окна, 0 — с окном (для отладки на ПК)
    "POLL_INTERVAL": "5",       # пауза воркера при пустой очереди, сек
    "TYPE_DELAY_MIN": "30",     # мин. задержка между символами, мс
    "TYPE_DELAY_MAX": "110",    # макс. задержка между символами, мс
    "NAV_TIMEOUT": "45",        # таймаут загрузки/ожидания элементов, сек
    "PAUSED": "0",              # 1 — воркер на паузе (заявки копятся в очереди)
    "MAX_PENDING_PER_USER": "3",  # антиспам: сколько заявок одного юзера может ждать в очереди
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("support_bot")

db: aiosqlite.Connection  # инициализируется в main()

router = Router()


# ----------------------------------------------------------------------------
# 4. БАЗА ДАННЫХ
# ----------------------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def init_db() -> None:
    global db
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            problem TEXT NOT NULL,
            full_name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',  -- pending|processing|sent|failed|manual
            attempts INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets(status, id);
        """
    )
    # Если процесс упал во время обработки — вернуть такие заявки в очередь
    await db.execute(
        "UPDATE tickets SET status='pending', updated_at=? WHERE status='processing'",
        (now_iso(),),
    )
    await db.commit()


async def cfg_get(key: str) -> str:
    async with db.execute("SELECT value FROM settings WHERE key=?", (key,)) as cur:
        row = await cur.fetchone()
    return row["value"] if row else DEFAULTS.get(key, "")


async def cfg_set(key: str, value: str) -> None:
    await db.execute(
        "INSERT INTO settings(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    await db.commit()


async def cfg_all() -> dict[str, str]:
    return {k: await cfg_get(k) for k in DEFAULTS}


async def ticket_create(user_id: int, username: Optional[str], data: dict) -> int:
    ts = now_iso()
    cur = await db.execute(
        "INSERT INTO tickets(user_id, username, problem, full_name, email, phone, "
        "status, created_at, updated_at) VALUES(?,?,?,?,?,?, 'pending', ?, ?)",
        (user_id, username, data["problem"], data["full_name"], data["email"],
         data["phone"], ts, ts),
    )
    await db.commit()
    return cur.lastrowid


async def ticket_set_status(tid: int, status: str, error: Optional[str] = None,
                            inc_attempt: bool = False) -> None:
    await db.execute(
        "UPDATE tickets SET status=?, error=?, attempts=attempts+?, updated_at=? WHERE id=?",
        (status, error, 1 if inc_attempt else 0, now_iso(), tid),
    )
    await db.commit()


async def ticket_take_next() -> Optional[aiosqlite.Row]:
    """Берёт самую старую pending-заявку и помечает её processing."""
    async with db.execute(
        "SELECT * FROM tickets WHERE status='pending' ORDER BY id LIMIT 1"
    ) as cur:
        row = await cur.fetchone()
    if row:
        await ticket_set_status(row["id"], "processing", inc_attempt=True)
    return row


# ----------------------------------------------------------------------------
# 5. УТИЛИТЫ УВЕДОМЛЕНИЙ
# ----------------------------------------------------------------------------
def esc(s: object) -> str:
    return html.escape(str(s))


async def get_admin_id() -> Optional[int]:
    v = (await cfg_get("ADMIN_ID")).strip()
    return int(v) if v.isdigit() else None


async def is_admin(user_id: int) -> bool:
    return (await get_admin_id()) == user_id


async def notify_admin(bot: Bot, text: str, photo: Optional[Path] = None,
                       markup: Optional[InlineKeyboardMarkup] = None) -> None:
    admin = await get_admin_id()
    if not admin:
        log.warning("ADMIN_ID не задан, уведомление пропущено: %s", text[:200])
        return
    try:
        if photo and Path(photo).exists():
            await bot.send_photo(admin, FSInputFile(photo), caption=f"📸 {Path(photo).name}")
        # Telegram ограничивает сообщение 4096 символами — режем на части,
        # кнопки вешаем на последнюю часть
        chunks = [text[i:i + 3900] for i in range(0, len(text), 3900)] or [text]
        for n, chunk in enumerate(chunks):
            await bot.send_message(admin, chunk,
                                   reply_markup=markup if n == len(chunks) - 1 else None)
    except Exception:
        log.exception("Не удалось отправить уведомление администратору")


# ----------------------------------------------------------------------------
# 6. FSM БОТА
# ----------------------------------------------------------------------------
class Form(StatesGroup):
    problem = State()
    full_name = State()
    email = State()
    phone = State()
    confirm = State()


EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
PHONE_RE = re.compile(r"^\+?[\d\s\-()]{7,20}$")


def kb_start() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Создать обращение", callback_data="new")],
        [InlineKeyboardButton(text="📋 Мои заявки", callback_data="my")],
    ])


def kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Подтвердить отправку", callback_data="confirm_yes"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="confirm_no"),
    ]])


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    # Первый пользователь становится администратором
    if not (await cfg_get("ADMIN_ID")).strip():
        await cfg_set("ADMIN_ID", str(message.from_user.id))
        extra = "\n\n👑 Вы первый пользователь — вы назначены администратором."
    else:
        extra = ""
    await message.answer(
        "Привет! Я помогу оформить обращение в поддержку." + extra,
        reply_markup=kb_start(),
    )


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Отменено.", reply_markup=kb_start())


@router.callback_query(F.data == "new")
async def cb_new(call: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(Form.problem)
    await call.message.answer("1/4. Опишите проблему текстом:")
    await call.answer()


@router.message(Form.problem, F.text)
async def st_problem(message: Message, state: FSMContext) -> None:
    text = message.text.strip()
    if len(text) < 10:
        await message.answer("Слишком коротко, опишите подробнее (минимум 10 символов).")
        return
    await state.update_data(problem=text)
    await state.set_state(Form.full_name)
    await message.answer("2/4. Введите полное имя (ФИО):")


@router.message(Form.full_name, F.text)
async def st_name(message: Message, state: FSMContext) -> None:
    name = message.text.strip()
    if len(name) < 2:
        await message.answer("Введите имя полностью.")
        return
    await state.update_data(full_name=name)
    await state.set_state(Form.email)
    await message.answer("3/4. Введите контактный email:")


@router.message(Form.email, F.text)
async def st_email(message: Message, state: FSMContext) -> None:
    email = message.text.strip()
    if not EMAIL_RE.match(email):
        await message.answer("Похоже на неверный email. Пример: name@example.com")
        return
    await state.update_data(email=email)
    await state.set_state(Form.phone)
    await message.answer("4/4. Введите номер телефона (например, +79991234567):")


@router.message(Form.phone, F.text)
async def st_phone(message: Message, state: FSMContext) -> None:
    phone = message.text.strip()
    if not PHONE_RE.match(phone):
        await message.answer("Неверный формат номера. Пример: +79991234567")
        return
    await state.update_data(phone=phone)
    data = await state.get_data()
    await state.set_state(Form.confirm)
    await message.answer(
        "Проверьте данные:\n\n"
        f"<b>Проблема:</b> {esc(data['problem'])}\n"
        f"<b>Имя:</b> {esc(data['full_name'])}\n"
        f"<b>Email:</b> {esc(data['email'])}\n"
        f"<b>Телефон:</b> {esc(data['phone'])}",
        reply_markup=kb_confirm(),
    )


@router.callback_query(Form.confirm, F.data == "confirm_yes")
async def cb_yes(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    data = await state.get_data()
    # Антиспам: ограничиваем число ожидающих заявок на пользователя
    limit = int(await cfg_get("MAX_PENDING_PER_USER"))
    async with db.execute(
        "SELECT COUNT(*) c FROM tickets WHERE user_id=? AND status IN ('pending','processing')",
        (call.from_user.id,),
    ) as cur:
        waiting = (await cur.fetchone())["c"]
    if waiting >= limit:
        await call.answer(f"В очереди уже {waiting} ваших заявок. Дождитесь обработки.",
                          show_alert=True)
        return
    await state.clear()
    tid = await ticket_create(call.from_user.id, call.from_user.username, data)
    async with db.execute(
        "SELECT COUNT(*) c FROM tickets WHERE status='pending' AND id<=?", (tid,)
    ) as cur:
        position = (await cur.fetchone())["c"]
    await call.message.edit_reply_markup(reply_markup=None)
    await call.message.answer(
        f"✅ Заявка №{tid} принята. Место в очереди: {position}.\n"
        "Когда начну отправку — покажу прогресс в процентах."
    )
    await call.answer()
    admin = await get_admin_id()
    if admin and admin != call.from_user.id:
        await notify_admin(bot, f"🆕 Новая заявка №{tid} от @{call.from_user.username or call.from_user.id}")


@router.callback_query(Form.confirm, F.data == "confirm_no")
async def cb_no(call: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await call.message.edit_reply_markup(reply_markup=None)
    await call.message.answer("Отправка отменена.", reply_markup=kb_start())
    await call.answer()


STATUS_LABELS = {
    "pending": "🕒 в очереди",
    "processing": "⚙️ отправляется",
    "sent": "✅ отправлена",
    "failed": "❌ ошибка",
    "manual": "🧩 нужна ручная обработка",
}


async def render_my(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    """Список последних заявок пользователя + кнопки отмены для ожидающих."""
    async with db.execute(
        "SELECT id, status, substr(problem,1,40) p FROM tickets WHERE user_id=? "
        "ORDER BY id DESC LIMIT 8", (user_id,)
    ) as cur:
        rows = await cur.fetchall()
    if not rows:
        return "У вас пока нет заявок.", kb_start()
    lines, buttons = ["📋 <b>Ваши заявки</b>\n"], []
    for r in rows:
        lines.append(f"№{r['id']} — {STATUS_LABELS.get(r['status'], r['status'])}\n<i>{esc(r['p'])}…</i>")
        if r["status"] == "pending":
            buttons.append([InlineKeyboardButton(
                text=f"🗑 Отменить №{r['id']}", callback_data=f"cancel:{r['id']}")])
    buttons.append([InlineKeyboardButton(text="📝 Новая заявка", callback_data="new")])
    return "\n\n".join(lines), InlineKeyboardMarkup(inline_keyboard=buttons)


@router.message(Command("my"))
async def cmd_my(message: Message) -> None:
    text, kb = await render_my(message.from_user.id)
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data == "my")
async def cb_my(call: CallbackQuery) -> None:
    text, kb = await render_my(call.from_user.id)
    await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("cancel:"))
async def cb_cancel_ticket(call: CallbackQuery) -> None:
    tid = int(call.data.split(":")[1])
    cur = await db.execute(
        "UPDATE tickets SET status='failed', error='отменена пользователем', updated_at=? "
        "WHERE id=? AND user_id=? AND status='pending'",
        (now_iso(), tid, call.from_user.id),
    )
    await db.commit()
    await call.answer("Заявка отменена" if cur.rowcount else "Её уже нельзя отменить",
                      show_alert=not cur.rowcount)


# ----------------------------------------------------------------------------
# 7. АДМИН-КОМАНДЫ
# ----------------------------------------------------------------------------
def kb_retry(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"🔁 Повторить №{tid}", callback_data=f"retry:{tid}")
    ]])


async def render_panel() -> tuple[str, InlineKeyboardMarkup]:
    async with db.execute("SELECT status, COUNT(*) c FROM tickets GROUP BY status") as cur:
        counts = {r["status"]: r["c"] for r in await cur.fetchall()}
    async with db.execute("SELECT COUNT(DISTINCT user_id) c FROM tickets") as cur:
        users = (await cur.fetchone())["c"]
    paused = (await cfg_get("PAUSED")) == "1"
    text = (
        "🎛 <b>Админ-панель</b>\n\n"
        f"Воркер: {'⏸ на паузе' if paused else '▶️ работает'}\n"
        f"Пользователей: {users}\n"
        + "\n".join(f"{STATUS_LABELS.get(k, k)}: {v}" for k, v in counts.items())
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Продолжить" if paused else "⏸ Пауза",
                              callback_data="panel:toggle")],
        [InlineKeyboardButton(text="🔁 Повторить все ошибки", callback_data="panel:retry_all")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="panel:refresh")],
    ])
    return text, kb


@router.message(Command("panel"))
async def cmd_panel(message: Message) -> None:
    if not await is_admin(message.from_user.id):
        return
    text, kb = await render_panel()
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("panel:"))
async def cb_panel(call: CallbackQuery) -> None:
    if not await is_admin(call.from_user.id):
        await call.answer()
        return
    action = call.data.split(":")[1]
    if action == "toggle":
        await cfg_set("PAUSED", "0" if (await cfg_get("PAUSED")) == "1" else "1")
    elif action == "retry_all":
        await db.execute(
            "UPDATE tickets SET status='pending', updated_at=? WHERE status IN ('failed','manual')",
            (now_iso(),))
        await db.commit()
    text, kb = await render_panel()
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except Exception:
        pass  # "message is not modified"
    await call.answer("Готово")


@router.callback_query(F.data.startswith("retry:"))
async def cb_retry(call: CallbackQuery) -> None:
    if not await is_admin(call.from_user.id):
        await call.answer()
        return
    tid = int(call.data.split(":")[1])
    await ticket_set_status(tid, "pending")
    await call.answer(f"№{tid} снова в очереди", show_alert=True)
@router.message(Command("cfg"))
async def cmd_cfg(message: Message) -> None:
    if not await is_admin(message.from_user.id):
        return
    lines = ["⚙️ <b>Настройки</b> (меняются: /set КЛЮЧ значение)\n"]
    for k, v in (await cfg_all()).items():
        lines.append(f"<b>{k}</b> = <code>{esc(v)}</code>")
    await message.answer("\n".join(lines))


@router.message(Command("set"))
async def cmd_set(message: Message) -> None:
    if not await is_admin(message.from_user.id):
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Использование: /set КЛЮЧ значение\nКлючи: " + ", ".join(DEFAULTS))
        return
    key, value = parts[1].upper(), parts[2].strip()
    if key not in DEFAULTS:
        await message.answer("Неизвестный ключ. Доступны: " + ", ".join(DEFAULTS))
        return
    await cfg_set(key, value)
    await message.answer(f"✅ <b>{key}</b> = <code>{esc(value)}</code>")


@router.message(Command("queue"))
async def cmd_queue(message: Message) -> None:
    if not await is_admin(message.from_user.id):
        return
    async with db.execute("SELECT status, COUNT(*) c FROM tickets GROUP BY status") as cur:
        rows = await cur.fetchall()
    text = "\n".join(f"{r['status']}: {r['c']}" for r in rows) or "Заявок пока нет."
    await message.answer("📊 <b>Очередь</b>\n" + text)


@router.message(Command("retry"))
async def cmd_retry(message: Message) -> None:
    if not await is_admin(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /retry ID")
        return
    await ticket_set_status(int(parts[1]), "pending")
    await message.answer(f"Заявка №{parts[1]} возвращена в очередь.")


# ----------------------------------------------------------------------------
# 8. АВТОМАТИЗАЦИЯ ФОРМЫ (Playwright)
# ----------------------------------------------------------------------------
class FormError(Exception):
    """Ошибка на конкретном шаге заполнения формы."""

    def __init__(self, step: str, message: str, screenshot: Optional[Path] = None,
                 url: str = ""):
        super().__init__(message)
        self.step = step
        self.message = message
        self.screenshot = screenshot
        self.url = url


class CaptchaDetected(FormError):
    """На странице обнаружена капча — нужна ручная обработка."""


class Progress:
    """Живое сообщение с прогресс-баром в процентах (редактируется по ходу отправки)."""

    def __init__(self, bot: Bot, chat_id: int, tid: int):
        self.bot, self.chat_id, self.tid = bot, chat_id, tid
        self.msg_id: Optional[int] = None
        self.pct = 0
        self.label = ""
        self._last_sent = 0.0
        self._last_text = ""

    @staticmethod
    def _bar(pct: int) -> str:
        filled = pct // 10
        return "▓" * filled + "░" * (10 - filled)

    async def _render(self, header: str) -> None:
        text = f"{header} №{self.tid}\n{self._bar(self.pct)} <b>{self.pct}%</b>\n{esc(self.label)}"
        if text == self._last_text:
            return
        try:
            if self.msg_id is None:
                m = await self.bot.send_message(self.chat_id, text)
                self.msg_id = m.message_id
            else:
                await self.bot.edit_message_text(text, chat_id=self.chat_id,
                                                 message_id=self.msg_id)
            self._last_text = text
            self._last_sent = time.monotonic()
        except Exception:
            pass  # лимиты Telegram / «not modified» — прогресс не критичен

    async def start(self) -> None:
        self.label = "Готовлюсь к отправке…"
        await self._render("⏳ Заявка")

    async def update(self, pct: float, label: Optional[str] = None, force: bool = False) -> None:
        self.pct = max(self.pct, min(100, int(pct)))  # процент только растёт
        if label:
            self.label = label
        # Telegram не любит частые правки одного сообщения — не чаще раза в 2 сек
        if force or time.monotonic() - self._last_sent >= 2.0:
            await self._render("⏳ Заявка")

    async def done(self) -> None:
        self.pct, self.label = 100, "Заявка отправлена в поддержку ✅"
        await self._render("✅ Заявка")

    async def fail(self, label: str) -> None:
        self.label = label
        await self._render("⚠️ Заявка")


async def _human_type(page: Page, selector: str, text: str, dmin: int, dmax: int,
                      timeout_ms: int,
                      tick: Optional[Callable[[], Awaitable[None]]] = None) -> None:
    """Клик по полю и посимвольный ввод со случайными задержками."""
    field = page.locator(selector).first
    await field.wait_for(state="visible", timeout=timeout_ms)
    await field.scroll_into_view_if_needed()
    await field.click()
    await asyncio.sleep(random.uniform(0.2, 0.6))
    await field.fill("")  # очистить, если там что-то есть
    for ch in text:
        await page.keyboard.type(ch)
        if tick:
            await tick()  # обновляем процент прогресса
        await asyncio.sleep(random.uniform(dmin, dmax) / 1000)
        if random.random() < 0.04:  # изредка — "задумчивая" пауза
            await asyncio.sleep(random.uniform(0.25, 0.7))
    await asyncio.sleep(random.uniform(0.3, 0.8))


async def _has_captcha(page: Page, selector: str) -> bool:
    try:
        if await page.locator(selector).count() > 0:
            return True
        body = (await page.inner_text("body")).lower()
        return any(s in body for s in (
            "verify you are human", "подтвердите, что вы не робот", "checking your browser",
        ))
    except Exception:
        return False


async def _snap(page: Optional[Page], tid: int, tag: str) -> Optional[Path]:
    if page is None:
        return None
    path = SHOTS_DIR / f"ticket{tid}_{tag}_{int(time.time())}.png"
    try:
        await page.screenshot(path=str(path), full_page=True)
        return path
    except Exception:
        log.exception("Не удалось сделать скриншот")
        return None


async def submit_form(ticket: aiosqlite.Row, logs: list[str], progress: Progress) -> None:
    """Заполняет и отправляет форму. Бросает FormError/CaptchaDetected при сбое."""
    c = await cfg_all()
    timeout_ms = int(c["NAV_TIMEOUT"]) * 1000
    dmin, dmax = int(c["TYPE_DELAY_MIN"]), int(c["TYPE_DELAY_MAX"])
    tid = ticket["id"]
    state = {"step": "launch"}

    def step(name: str) -> None:
        state["step"] = name
        logs.append(f"{datetime.now().strftime('%H:%M:%S')} шаг: {name}")
        log.info("[ticket %s] %s", tid, name)

    # Подсчёт символов для процента: ввод данных занимает диапазон 15–85 %
    total_chars = max(1, len(ticket["problem"]) + len(ticket["full_name"])
                      + len(ticket["email"]) + len(ticket["phone"]))
    typed = 0

    async def tick() -> None:
        nonlocal typed
        typed += 1
        await progress.update(15 + 70 * typed / total_chars, "Ввожу данные в форму…")

    page: Optional[Page] = None
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=c["HEADLESS"] != "0")
        try:
            context = await browser.new_context(
                locale="ru-RU", viewport={"width": 1280, "height": 900}
            )
            page = await context.new_page()
            page.set_default_timeout(timeout_ms)

            step("open_page")
            await progress.update(3, "Открываю страницу поддержки…", force=True)
            await page.goto(c["URL"], wait_until="domcontentloaded")
            await progress.update(10, "Страница загружена")
            await asyncio.sleep(random.uniform(1.0, 2.0))

            step("captcha_check_before")
            await progress.update(12, "Проверяю страницу…")
            if await _has_captcha(page, c["SEL_CAPTCHA"]):
                raise CaptchaDetected(
                    state["step"], "Капча/проверка браузера обнаружена до заполнения формы",
                    await _snap(page, tid, "captcha"), page.url)

            fields = [
                ("fill_problem", c["SEL_PROBLEM"], ticket["problem"]),
                ("fill_name", c["SEL_NAME"], ticket["full_name"]),
                ("fill_email", c["SEL_EMAIL"], ticket["email"]),
                ("fill_phone", c["SEL_PHONE"], ticket["phone"]),
            ]
            for name, selector, value in fields:
                step(name)
                await _human_type(page, selector, value, dmin, dmax, timeout_ms, tick)

            step("captcha_check_after_fill")
            if await _has_captcha(page, c["SEL_CAPTCHA"]):
                raise CaptchaDetected(
                    state["step"], "Капча появилась после заполнения формы",
                    await _snap(page, tid, "captcha"), page.url)

            step("click_submit")
            await progress.update(88, "Нажимаю «Отправить»…", force=True)
            submit = page.locator(c["SEL_SUBMIT"]).first
            await submit.wait_for(state="visible", timeout=timeout_ms)
            await submit.scroll_into_view_if_needed()
            await asyncio.sleep(random.uniform(0.5, 1.2))
            await submit.click()

            step("wait_result")
            await progress.update(93, "Жду ответ сервера…", force=True)
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass  # страница могла не уходить в networkidle — проверим по тексту
            await asyncio.sleep(2)

            if await _has_captcha(page, c["SEL_CAPTCHA"]):
                raise CaptchaDetected(
                    state["step"], "Капча появилась после нажатия «Отправить»",
                    await _snap(page, tid, "captcha"), page.url)

            step("confirm_success")
            await progress.update(97, "Проверяю результат…", force=True)
            body = (await page.inner_text("body")).lower()
            markers = [s.strip().lower() for s in c["SUCCESS_TEXT"].split(",") if s.strip()]
            if not any(m in body for m in markers):
                raise FormError(
                    state["step"],
                    "Не удалось подтвердить успешную отправку (не найден текст из SUCCESS_TEXT). "
                    "Форма могла и уйти — проверьте скриншот и при необходимости поправьте SUCCESS_TEXT.",
                    await _snap(page, tid, "unconfirmed"), page.url)
            logs.append("успех подтверждён по тексту на странице")

        except FormError:
            raise
        except Exception as e:
            shot = await _snap(page, tid, "error")
            raise FormError(state["step"], f"{type(e).__name__}: {e}", shot,
                            page.url if page else "")
        finally:
            await browser.close()


async def process_ticket(bot: Bot, ticket: aiosqlite.Row) -> None:
    tid = ticket["id"]
    logs: list[str] = []
    progress = Progress(bot, ticket["user_id"], tid)
    await progress.start()
    try:
        await submit_form(ticket, logs, progress)
        await ticket_set_status(tid, "sent")
        await progress.done()
        await notify_admin(bot, f"✅ Заявка №{tid} успешно отправлена.")
    except CaptchaDetected as e:
        await ticket_set_status(tid, "manual", error=e.message)
        await progress.fail("Нужна ручная обработка — администратор уведомлён.")
        await notify_admin(bot, _report(tid, "🧩 КАПЧА — нужно ручное вмешательство", e, logs),
                           e.screenshot, kb_retry(tid))
    except FormError as e:
        await ticket_set_status(tid, "failed", error=f"{e.step}: {e.message}")
        await progress.fail("Не удалось отправить — администратор уведомлён.")
        await notify_admin(bot, _report(tid, "❌ СБОЙ ОТПРАВКИ", e, logs),
                           e.screenshot, kb_retry(tid))
    except Exception as e:  # непредвиденная ошибка воркера
        log.exception("Необработанная ошибка в заявке %s", tid)
        await ticket_set_status(tid, "failed", error=repr(e))
        await progress.fail("Внутренняя ошибка — администратор уведомлён.")
        await notify_admin(bot, f"❌ Заявка №{tid}: непредвиденная ошибка воркера\n"
                                f"{type(e).__name__}: {e}", None, kb_retry(tid))


def _report(tid: int, title: str, e: FormError, logs: list[str]) -> str:
    return (
        f"{title}\n"
        f"Заявка: №{tid}\n"
        f"Шаг: {e.step}\n"
        f"Причина: {e.message}\n"
        f"URL: {e.url or '-'}\n"
        f"Скриншот: {e.screenshot.name if e.screenshot else 'нет'}\n"
        f"Что делать: /retry {tid} — повторить; /cfg и /set — поправить селекторы.\n\n"
        "Лог:\n" + "\n".join(logs[-15:])
    )


async def _safe_send(bot: Bot, chat_id: int, text: str) -> None:
    try:
        await bot.send_message(chat_id, text)
    except Exception:
        log.warning("Не удалось отправить сообщение пользователю %s", chat_id)


async def worker(bot: Bot) -> None:
    """Фоновый цикл: берёт заявки из очереди по одной."""
    log.info("Воркер запущен")
    while True:
        try:
            if (await cfg_get("PAUSED")) == "1":
                await asyncio.sleep(int(await cfg_get("POLL_INTERVAL")))
                continue
            ticket = await ticket_take_next()
            if ticket is None:
                await asyncio.sleep(int(await cfg_get("POLL_INTERVAL")))
                continue
            await process_ticket(bot, ticket)
            await asyncio.sleep(random.uniform(3, 8))  # пауза между заявками
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Ошибка в цикле воркера")
            await asyncio.sleep(10)


# ----------------------------------------------------------------------------
# 9. ТОЧКА ВХОДА
# ----------------------------------------------------------------------------
async def main() -> None:
    if not BOT_TOKEN:
        sys.exit("Не задан BOT_TOKEN. Запуск: BOT_TOKEN=123:ABC python bot.py")
    await init_db()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    worker_task = asyncio.create_task(worker(bot))
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        worker_task.cancel()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
