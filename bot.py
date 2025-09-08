import os, re, csv, uuid, sqlite3
from datetime import time, datetime
from typing import Dict, Any
from hashlib import md5
from telegram.constants import ChatType
from telegram import ReplyKeyboardMarkup, KeyboardButton

import pytz
from dotenv import load_dotenv

from telegram import (
    Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)
from telegram.request import HTTPXRequest
from telegram.error import NetworkError, RetryAfter, TimedOut

from hashlib import md5   # add this with your imports

CATEGORY_ID_MAP = {}      # 🔹 add this near the top, a global dictionary

# ==================== ENV / CONFIG ====================
load_dotenv()

def _mask_token(t: str) -> str:
    if not t or len(t) < 10: return "(missing)"
    return t[:6] + "..." + t[-6:]

def _require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

BOT_TOKEN = _require_env("BOT_TOKEN")
WORKERS_CHAT_ID = int(os.getenv("WORKERS_CHAT_ID", "0"))
CLIENT_GROUP_IDS = [int(x.strip()) for x in os.getenv("CLIENT_GROUP_IDS", "").split(",") if x.strip()]
OWNER_USER_ID = int(os.getenv("OWNER_USER_ID", "0"))  # optional
TZ_NAME = os.getenv("TZ", "Asia/Tashkent")
MORNING_HOUR = int(os.getenv("MORNING_HOUR", "9"))
EVENING_HOUR = int(os.getenv("EVENING_HOUR", "21"))

# optional pricing rules
DELIVERY_FEE = int(os.getenv("DELIVERY_FEE", "0") or 0)
FREE_SHIPPING_OVER = int(os.getenv("FREE_SHIPPING_OVER", "0") or 0)
DISCOUNT_PERCENT = int(os.getenv("DISCOUNT_PERCENT", "0") or 0)

TZ = pytz.timezone(TZ_NAME)

ORDERS_CSV = "orders.csv"
DB = "products.db"
PAGE_SIZE = 6  # products per page

# ==================== UI TEXT (UZ) ====================
MAIN_MENU = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🛒 Katalog"), KeyboardButton("🧺 Savatcha")],
        [KeyboardButton("🛍 Buyurtma berish"), KeyboardButton("❓ Savollar (FAQ)")],
        [KeyboardButton("📍 Manzilimiz"), KeyboardButton("🧑‍🍳 Operator bilan bog‘lanish")],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,   # keep visible
)

FAQ_TEXT = (
    "<b>Ko‘p so‘raladigan savollar</b>\n\n"
    "• Yetkazib berish: Toshkent ichida ko`rsatilgan manzilga qarab 1 soatdan 3 soatgacha.\n"
    "• To‘lov: Naqd/POS/karta/Click/Payme.\n"
    "• Qaytarish: tovarni qaytarib olish muddati yo`q, faqat qadoq buzilmagan bo‘lsa bo`ldi.\n"
    "• Ish vaqti: Dushanba–Yakshanba 10:00–19:00, dam olish kunlarisiz."
)

PHONE_KB = ReplyKeyboardMarkup(
    [[KeyboardButton("📞 Raqamni ulashish", request_contact=True)]],
    resize_keyboard=True, one_time_keyboard=True
)

# ==================== STATE ====================
user_state: Dict[int, Dict[str, Any]] = {}

import os, csv
from datetime import datetime

from telegram.constants import ChatType
from telegram import ReplyKeyboardMarkup, KeyboardButton

def location_keyboard_for(chat) -> ReplyKeyboardMarkup:
    """PRIVATE chat -> true share-location button; GROUPS -> simple keyboard."""
    if chat and chat.type == ChatType.PRIVATE:
        return ReplyKeyboardMarkup(
            [[KeyboardButton("📍 Manzilimni ulashish", request_location=True)],
             ["◀️ Ortga"]],
            resize_keyboard=True,
            one_time_keyboard=False
        )
    else:
        return ReplyKeyboardMarkup(
            [["◀️ Ortga"]],
            resize_keyboard=True,
            one_time_keyboard=False
        )

def save_order_row(row: Dict[str, Any]):
    file_exists = os.path.exists(ORDERS_CSV)
    # master schema — now includes 'location'
    fieldnames = [
        "time",
        "user_id",
        "username",
        "name",
        "phone",
        "address",
        "location",   # ← NEW
        "items",
        "note",
        "total",
        "status",
    ]

    # add time and write safely
    row = dict(row)
    row.setdefault("time", datetime.now().isoformat(timespec="seconds"))

    with open(ORDERS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

# ==================== DB HELPERS ====================
def db_conn():
    return sqlite3.connect(DB)

def location_request_keyboard():
    kb = [[KeyboardButton("📍 Manzilimni ulashish", request_location=True)],
          ["◀️ Ortga"]]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True)

def list_categories():
    if not os.path.exists(DB): return []
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(category,'Other') AS c, COUNT(*) FROM products GROUP BY c ORDER BY c")
        return [{"category": r[0], "count": r[1]} for r in cur.fetchall()]

def category_id(name: str) -> str:
    """
    Return a short, stable ID for a category (10 hex chars).
    Also store mapping for reverse lookup.
    """
    if not name:
        name = "Other"
    cid = md5(name.encode("utf-8")).hexdigest()[:10]
    CATEGORY_ID_MAP[cid] = name
    return cid

def get_product(sku):
    if not os.path.exists(DB): return None
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""SELECT sku,title,price,category,subcategory,description,image_url,image_path,stock
                       FROM products WHERE sku=?""", (sku,))
        r = cur.fetchone()
        if not r: return None
        keys = ["sku","title","price","category","subcategory","description","image_url","image_path","stock"]
        return dict(zip(keys, r))

def search_products(q, limit=PAGE_SIZE, offset=0, category=None):
    if not os.path.exists(DB): return []
    q_like = f"%{q}%" if q else "%"
    sql = "SELECT sku,title,price FROM products WHERE (title LIKE ? OR sku LIKE ?)"
    args = [q_like, q_like]
    if category:
        sql += " AND category=?"
        args.append(category)
    sql += " ORDER BY title LIMIT ? OFFSET ?"
    args += [limit, offset]
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, args)
        return [{"sku":a, "title":b, "price":c} for (a,b,c) in cur.fetchall()]

def category_id(name: str) -> str:
    """
    Make a short, stable ID for a category (<=64 bytes safe).
    Stores mapping for reverse lookup when user clicks buttons.
    """
    if not name:
        name = "Other"
    cid = md5(name.encode("utf-8")).hexdigest()[:10]
    CATEGORY_ID_MAP[cid] = name
    return cid

def compute_cart_total(cart):
    total = 0
    lines = []
    for item in cart:
        p = get_product(item["sku"])
        if not p: 
            continue
        qty = item["qty"]
        total += p["price"] * qty
        lines.append(f"{p['title']} x{qty} — {p['price']} so‘m")
    return total, lines

def apply_pricing_rules(subtotal: int):
    discount = (subtotal * DISCOUNT_PERCENT) // 100 if DISCOUNT_PERCENT else 0
    after = max(0, subtotal - discount)
    delivery = 0 if (FREE_SHIPPING_OVER and after >= FREE_SHIPPING_OVER) else DELIVERY_FEE
    grand = after + delivery
    return {"subtotal": subtotal, "discount": discount, "delivery": delivery, "total": grand}

# ==================== GENERIC SAFE SEND ====================
async def safe_send_message(bot, chat_id, **kwargs):
    try:
        return await bot.send_message(chat_id=chat_id, **kwargs)
    except (RetryAfter, TimedOut, NetworkError) as e:
        print(f"[warn] send_message failed: {e}")

async def safe_send_photo(bot, chat_id, **kwargs):
    try:
        return await bot.send_photo(chat_id=chat_id, **kwargs)
    except (RetryAfter, TimedOut, NetworkError) as e:
        print(f"[warn] send_photo failed: {e}")
        # fallback to text if caption exists
        cap = kwargs.get("caption")
        if cap:
            await safe_send_message(bot, chat_id, text=cap)

# ==================== BASIC COMMANDS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_html(
        "Salom! Men <b>Santan</b> do‘konining yordamchi botiman. Katalog yoki buyurtma bilan yordam beraman 👇",
        reply_markup=MAIN_MENU
    )

async def chatid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Chat ID: {update.effective_chat.id}")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if OWNER_USER_ID and u.id != OWNER_USER_ID:
        return
    cats = list_categories()
    msg = (
        "<b>Bot holati</b>\n"
        f"Token: <code>{_mask_token(BOT_TOKEN)}</code>\n"
        f"Workers chat: <code>{WORKERS_CHAT_ID}</code>\n"
        f"Client groups: <code>{','.join(map(str, CLIENT_GROUP_IDS)) or '(none)'}</code>\n"
        f"TZ: <code>{TZ_NAME}</code> | Morning: <code>{MORNING_HOUR}:00</code> | Evening: <code>{EVENING_HOUR}:00</code>\n"
        f"Products DB: <code>{'mavjud' if cats else 'yo‘q yoki bo‘sh'}</code>"
    )
    await update.message.reply_html(msg)

async def faq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_html(FAQ_TEXT, reply_markup=MAIN_MENU)

async def contact_operator(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Xabaringiz operatorga uzatildi. Tez orada javob beramiz.")
    if WORKERS_CHAT_ID:
        u = update.effective_user
        txt = (f"🆘 <b>Mijoz operator so‘radi</b>\n"
               f"ID: <code>{u.id}</code>\nUsername: @{u.username}\nIsm: {u.full_name}")
        await safe_send_message(context.bot, WORKERS_CHAT_ID, text=txt, parse_mode="HTML")

# ==================== ORDER FLOW ====================
async def order_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    user_state[uid] = {"step":"items"}
    await update.message.reply_text(
        "Buyurtma beramiz. Qaysi mahsulot(lar) va miqdor(lar)ni yozing.\n"
        "Masalan:\n- Dush geli x2\n- Tualet qog‘ozi x3\n- Sovun x1",
        reply_markup=ReplyKeyboardRemove()
    )

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    uid = update.effective_user.id
    state = user_state.get(uid)

    low = (msg.text or "").strip().lower()
    if low in ["❓ savollar (faq)", "faq", "savol", "savollar"]:
        return await faq(update, context)
    if low in ["🧑‍🍳 operator bilan bog‘lanish", "operator", "admin"]:
        return await contact_operator(update, context)
    if low in ["🛍 buyurtma berish", "buyurtma", "order"]:
        return await order_start(update, context)
    if low in ["🛒 katalog", "katalog"]:
        return await cmd_catalog(update, context)
    if low in ["🧺 savatcha", "savatcha"]:
        return await view_cart(update, context)
    if low in ["📍 manzilimiz", "manzilimiz", "/location", "location"]:
        return await cmd_location(update, context)
    
    if not state:
        return  # ignore unrelated text

    if state.get("step") == "items":
        state["items"] = msg.text
        kb = location_keyboard_for(update.effective_chat)
        note = ""
        if update.effective_chat.type != ChatType.PRIVATE:
            note = (
                "\n\nℹ️ Lokatsiya tugmasi faqat shaxsiy chatda ishlaydi. "
                "Iltimos botga private yozing yoki manzilni yozib yuboring."
    )
        await update.message.reply_text(
            "Manzilingizni kiriting yoki (shaxsiy chatda) pastdagi tugma orqali lokatsiya yuboring:" + note,
            reply_markup=kb
        )
        state["step"] = "address"
        return

    if state.get("step") == "address":
        state["address"] = msg.text
        state["step"] = "phone"
        await update.message.reply_text(
    "Telefon raqamingizni yuboring (masalan, +998 ** *** ** **):",
    reply_markup=ReplyKeyboardMarkup([["◀️ Ortga"]], resize_keyboard=True))
        return

    if state.get("step") == "phone":
        phone = msg.text.strip()
        if not re.match(r"^\+?\d{9,15}$", phone):
            await msg.reply_text("Telefon raqam formati noto‘g‘ri. Iltimos, +99890xxxxxxx ko‘rinishida yuboring.")
            return
        state["phone"] = phone
        state["step"] = "note"
        await msg.reply_text("Qo‘shimcha izoh bormi? (yo‘q bo‘lsa, ‘Yo‘q’ deb yozing)", reply_markup=ReplyKeyboardRemove())
        return

    if state.get("step") == "note":
        state["note"] = msg.text
        state["step"] = "confirm"
        # if user used catalog, we may have a computed total already
        cart_total = state.get("cart_total")
        total_line = f"\n<b>Jami:</b> {cart_total} so‘m" if cart_total else ""
        summary = (
    "<b>Buyurtma xulosasi</b>\n\n"
    f"<b>Mahsulotlar:</b> {state['items']}\n"
    f"<b>Manzil:</b> {state['address']}\n"
    f"<b>Telefon:</b> {state['phone']}\n"
    f"<b>Izoh:</b> {state['note']}{total_line}\n"
    f"<b>Lokatsiya:</b> {state.get('location', '❌ yuborilmagan')}\n\n"
    "Buyurtmani tasdiqlaysizmi?"
)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Ha", callback_data="confirm_yes"),
             InlineKeyboardButton("✏️ Yo‘q, o‘zgartiraman", callback_data="confirm_no")]
        ])
        await msg.reply_html(summary, reply_markup=kb)
        return

async def on_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    state = user_state.get(uid)
    if not state or state.get("step") != "phone":
        return
    c = update.message.contact
    phone = c.phone_number if c.phone_number.startswith("+") else f"+{c.phone_number}"
    state["phone"] = phone
    state["step"] = "note"
    await update.message.reply_text(
        "Rahmat! Telefon qabul qilindi. Qo‘shimcha izoh bormi? (yo‘q bo‘lsa, ‘Yo‘q’ deb yozing)",
        reply_markup=ReplyKeyboardRemove()
    )

async def confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = q.from_user.id
    state = user_state.get(uid)
    await q.answer()

    # guard
    if not state or state.get("step") != "confirm":
        return

    if q.data == "confirm_yes":
        u = q.from_user

        # normalize fields
        addr = state.get("address", "yo‘q")
        phone = state.get("phone", "yo‘q")
        note = state.get("note", "—")
        items = state.get("items", "—")
        cart_total = state.get("cart_total", "(aniqlanmagan)")

        # location may be "lat,lon" (str) or (lat,lon) tuple -> normalize to "lat,lon"
        loc_str = state.get("location")
        if isinstance(loc_str, (tuple, list)):
            try:
                loc_str = f"{float(loc_str[0]):.6f},{float(loc_str[1]):.6f}"
            except Exception:
                loc_str = None
        elif isinstance(loc_str, str):
            loc_str = loc_str.strip() or None

        # Build workers message
        text = (
            "🆕 <b>Yangi buyurtma</b>\n\n"
            f"👤 <b>Mijoz:</b> {u.full_name} "
            f"(@{u.username}) | ID: <code>{u.id}</code>\n"
            f"🛒 <b>Mahsulotlar:</b> {items}\n"
            f"🏠 <b>Manzil:</b> {addr}\n"
            f"📞 <b>Telefon:</b> {phone}\n"
            f"📝 <b>Izoh:</b> {note}\n"
            f"💰 <b>Jami:</b> {cart_total} so‘m"
        )

        # Add location + map link if present
        if loc_str:
            text += f"\n🗺️ <b>Lokatsiya:</b> {loc_str}\n"
            text += f"🔗 <a href='https://maps.google.com/?q={loc_str}'>Xaritada ochish</a>"

        # Send to workers group
        if WORKERS_CHAT_ID:
            await safe_send_message(
                context.bot, WORKERS_CHAT_ID, text=text, parse_mode="HTML"
            )
            # (Optional) also drop a map pin in the group
            if loc_str:
                try:
                    lat, lon = map(float, loc_str.split(","))
                    await context.bot.send_venue(
                        chat_id=WORKERS_CHAT_ID,
                        latitude=lat,
                        longitude=lon,
                        title=f"Mijoz lokatsiyasi — {u.full_name}",
                        address=addr or "Mijoz lokatsiyasi",
                    )
                except Exception:
                    pass

        # Save order row (add location)
        try:
            save_order_row({
        "user_id": u.id,
        "username": u.username,
        "name": u.full_name,
        "phone": phone,
        "address": addr,
        "location": loc_str or "",   # keep this!
        "items": items,
        "note": note,
        "total": cart_total,
        "status": "Yuborildi",
    })
        except Exception as e:
            print("save_order_row error:", e)

        # Confirm to client
        await q.edit_message_text("Rahmat! Buyurtmangiz qabul qilindi va ishlov berilmoqda ✅")
        user_state.pop(uid, None)
        await context.bot.send_message(
            chat_id=q.from_user.id,
            text="Bosh menyu ⬇️",
            reply_markup=MAIN_MENU,)
        return

    if q.data == "confirm_no":
        user_state[uid]["step"] = "items"
        await q.edit_message_text(
            "❌ Buyurtma bekor qilindi.\n\nKeling, qaytadan kiritamiz. Qaysi mahsulot(lar) va miqdorini yozing."
    )
        await context.bot.send_message(
            chat_id=q.from_user.id,
            text="Bosh menyu ⬇️",
            reply_markup=MAIN_MENU,)
        return

# ==================== CATALOG / SEARCH / CART ====================
async def cmd_catalog(update, context):
    cats = list_categories()
    if not cats:
        return await update.message.reply_text("Katalog hozircha bo‘sh.")
    rows = []
    for c in cats:
        cat = c["category"] or "Other"
        cid = category_id(cat)  # short ID for callback (fixes Button_data_invalid)
        rows.append([InlineKeyboardButton(f"{cat} ({c['count']})", callback_data=f"CAT|{cid}|0")])
    await update.message.reply_text("Bo‘limni tanlang:", reply_markup=InlineKeyboardMarkup(rows))

async def cmd_start(update, context):
    await update.message.reply_text(
        "Assalomu alaykum! 👋 Santan botiga xush kelibsiz.\nQuyidagi menyudan birini tanlang:",
        reply_markup=MAIN_MENU,
    )

async def cmd_menu(update, context):
    await update.message.reply_text(
        "Bosh menyu ⬇️",
        reply_markup=MAIN_MENU,
    )

async def on_location(update, context):
    loc = update.message.location
    lat, lon = loc.latitude, loc.longitude

    uid = update.effective_user.id
    state = user_state.setdefault(uid, {})   # use user_state (NOT context.user_data)
    step = state.get("step", "")

    # store normalized coords for reuse
    state["location"] = f"{lat:.6f},{lon:.6f}"

    if step == "address":
        state["address"] = f"📍 Geolokatsiya: {lat:.6f},{lon:.6f}"
        await update.message.reply_text(
            "✅ Lokatsiya qabul qilindi.\nIltimos telefon raqamingizni yuboring:"
        )
        state["step"] = "phone"
    else:
        await update.message.reply_text(f"Rahmat! Lokatsiya olindi: {lat:.6f},{lon:.6f}")

async def cmd_find(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = " ".join(context.args) if context.args else ""
    if not q:
        return await update.message.reply_text("Qidirish uchun: /find <matn yoki SKU>")
    items = search_products(q, limit=PAGE_SIZE, offset=0)
    if not items:
        return await update.message.reply_text("Hech narsa topilmadi.")
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton(f"{it['title']} — {it['price']} so‘m", callback_data=f"PROD|{it['sku']}")]
         for it in items]
    )
    await update.message.reply_text("Natijalar:", reply_markup=kb)

async def cmd_location(update, context):
    # Shop pin + link
    await update.message.reply_venue(
        latitude=float(os.getenv("SHOP_LAT", "41.372386")),
        longitude=float(os.getenv("SHOP_LON", "69.323775")),
        title=os.getenv("SHOP_NAME", "Santan"),
        address=os.getenv("SHOP_ADDRESS", "Manzil"),
    )
    maps = f"https://maps.google.com/?q={os.getenv('SHOP_LAT','41.372386')},{os.getenv('SHOP_LON','69.323775')}"
    await update.message.reply_text(
        f"📍 {os.getenv('SHOP_NAME','Santan')}\n{os.getenv('SHOP_ADDRESS','Manzil')}\n\n🔗 Xarita: {maps}"
    )

async def on_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    loc = update.message.location
    lat, lon = loc.latitude, loc.longitude

    uid = update.effective_user.id
    state = user_state.setdefault(uid, {})
    step = state.get("step", "")

    # Store normalized "lat,lon" for reuse
    state["location"] = f"{lat:.6f},{lon:.6f}"

    if step == "address":
        # We’re in checkout: accept location instead of typed address
        state["address"] = f"📍 Geolokatsiya: {lat:.6f},{lon:.6f}"
        await update.message.reply_text(
            "✅ Lokatsiya qabul qilindi.\nIltimos telefon raqamingizni yuboring:"
        )
        state["step"] = "phone"
    else:
        # If user sends location at other times
        await update.message.reply_text(f"Rahmat! Lokatsiya olindi: {lat:.6f},{lon:.6f}")
async def show_category_page(q, cid, page):
    page = max(0, int(page))
    category = CATEGORY_ID_MAP.get(cid)
    if not category:
        # If bot restarted and map is empty, ask user to reopen /catalog
        return await q.edit_message_text("Katalog yangilandi. Iltimos, /catalog ni qaytadan oching.")

    offset = page * PAGE_SIZE
    items = search_products("", limit=PAGE_SIZE, offset=offset, category=category)
    if not items:
        return await q.edit_message_text("Bu bo‘limda mahsulot topilmadi.")

    rows = [[InlineKeyboardButton(f"{it['title']} — {it['price']} so‘m", callback_data=f"PROD|{it['sku']}")]
            for it in items]

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Oldingi", callback_data=f"CAT|{cid}|{page-1}"))
    nav.append(InlineKeyboardButton("🧺 Savatcha", callback_data="CART|VIEW"))
    if len(items) == PAGE_SIZE:
        nav.append(InlineKeyboardButton("▶️ Keyingi", callback_data=f"CAT|{cid}|{page+1}"))
    rows.append(nav)

    await q.edit_message_text(f"{category} — mahsulotlar:", reply_markup=InlineKeyboardMarkup(rows))

async def send_product_card(chat_id, p, context, reply_to=None):
    cap = f"{p['title']}\nNarx: {p['price']} so‘m\nSKU: {p['sku']}"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Savatchaga", callback_data=f"ADD|{p['sku']}")],
        [InlineKeyboardButton("🧺 Savatcha", callback_data="CART|VIEW")]
    ])
    if p.get("image_url"):
        await safe_send_photo(context.bot, chat_id, photo=p["image_url"], caption=cap, reply_markup=kb)
    elif p.get("image_path") and os.path.exists(p["image_path"]):
        with open(p["image_path"], "rb") as f:
            await safe_send_photo(context.bot, chat_id, photo=f, caption=cap, reply_markup=kb)
    else:
        await safe_send_message(context.bot, chat_id, text=cap, reply_markup=kb)

async def view_cart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    state = user_state.setdefault(uid, {})
    cart = state.get("cart", [])
    if not cart:
        return await update.message.reply_text("Savatcha bo‘sh.")
    subtotal, lines = compute_cart_total(cart)
    totals = apply_pricing_rules(subtotal)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Rasmiylashtirish", callback_data="CART|CHECKOUT")]])
    txt = ("🧺 <b>Savatcha</b>\n" + "\n".join(lines) +
           f"\n\nOraliq: {totals['subtotal']} so‘m"
           f"\nChegirma: {totals['discount']} so‘m"
           f"\nYetkazib berish: {totals['delivery']} so‘m"
           f"\n<b>Jami: {totals['total']} so‘m</b>")
    await update.message.reply_html(txt, reply_markup=kb)

async def view_cart_inline(q, context):
    uid = q.from_user.id
    state = user_state.setdefault(uid, {})
    cart = state.get("cart", [])
    if not cart:
        return await q.edit_message_text("Savatcha bo‘sh.")
    subtotal, lines = compute_cart_total(cart)
    totals = apply_pricing_rules(subtotal)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Rasmiylashtirish", callback_data="CART|CHECKOUT")]])
    txt = ("🧺 Savatcha\n" + "\n".join(lines) +
           f"\n\nOraliq: {totals['subtotal']} so‘m"
           f"\nChegirma: {totals['discount']} so‘m"
           f"\nYetkazib berish: {totals['delivery']} so‘m"
           f"\nJami: {totals['total']} so‘m")
    await q.edit_message_text(txt, reply_markup=kb)

async def catalog_callback(update, context):
    q = update.callback_query
    await q.answer()
    data = q.data or ""

    if data.startswith("CAT|"):
        _, cid, page = data.split("|", 2)
        return await show_category_page(q, cid, page)

    if data.startswith("PROD|"):
        _, sku = data.split("|", 1)
        p = get_product(sku)
        if not p: return await q.answer("Topilmadi", show_alert=True)
        return await send_product_card(q.message.chat_id, p, context, reply_to=q)

    if data.startswith("ADD|"):
        _, sku = data.split("|", 1)
        uid = q.from_user.id
        state = user_state.setdefault(uid, {})
        cart = state.setdefault("cart", [])
        row = next((x for x in cart if x["sku"] == sku), None)
        if row: row["qty"] += 1
        else: cart.append({"sku": sku, "qty": 1})
        return await q.answer("Savatchaga qo‘shildi ✅")

    if data == "CART|VIEW":
        return await view_cart_inline(q, context)

    if data == "CART|CHECKOUT":
        uid = q.from_user.id
        state = user_state.setdefault(uid, {})
        cart = state.get("cart", [])
        if not cart:
            return await q.answer("Savatcha bo‘sh", show_alert=True)
        subtotal, lines = compute_cart_total(cart)
        totals = apply_pricing_rules(subtotal)
        state["items"] = "; ".join(lines)
        state["cart_total"] = totals["total"]
        state["step"] = "address"
        return await q.edit_message_text("Yetkazib berish manzilini yozing (ko‘cha, uy, mo‘ljal).")

# ==================== BROADCASTS ====================
async def morning_broadcast(context: ContextTypes.DEFAULT_TYPE):
    msg = "Assalomu alaykum! Bugungi kuningizda ishingizga rivoj va barokat tilab qolamiz! 😊 Bugun qanday buyurtma beramiz? Chegirmalar va yangi kelganlar haqida so‘rashingiz mumkin."
    for gid in CLIENT_GROUP_IDS:
        await safe_send_message(context.bot, gid, text=msg)

async def evening_broadcast(context: ContextTypes.DEFAULT_TYPE):
    msg = "Tuningiz xayrli o‘tsin! 🌙 Agar ertangi kun uchun buyurtma qilmoqchi bo‘lsangiz, yozib qoldiring. Santan jamoasi siz bilan 24/7 birga!"
    for gid in CLIENT_GROUP_IDS:
        await safe_send_message(context.bot, gid, text=msg)

# ==================== MAIN ====================
def main():
    # add timeouts so temporary network hiccups don’t crash
    request = HTTPXRequest(connect_timeout=30, read_timeout=30)
    app = Application.builder().token(BOT_TOKEN).request(request).build()

    # commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("chatid", chatid))
    app.add_handler(CommandHandler("faq", faq))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("catalog", cmd_catalog))
    app.add_handler(CommandHandler("find", cmd_find))
    app.add_handler(CommandHandler("location", cmd_location))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_menu))

    # messages & callbacks
    app.add_handler(CallbackQueryHandler(confirm_callback, pattern=r"^confirm_"))
    app.add_handler(CallbackQueryHandler(catalog_callback, pattern=r"^(CAT|PROD|ADD|CART)\|"))
    app.add_handler(MessageHandler(filters.CONTACT, on_contact))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_handler(MessageHandler(filters.LOCATION, on_location))

    # schedulers
    jq = app.job_queue
    if jq is None:
        print('⚠️ Install job-queue extra: pip install "python-telegram-bot[job-queue]==20.7"')
    else:
        jq.run_daily(morning_broadcast, time=time(MORNING_HOUR, 0, tzinfo=TZ))
        jq.run_daily(evening_broadcast, time=time(EVENING_HOUR, 0, tzinfo=TZ))

    print("Bot ishga tushdi…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
