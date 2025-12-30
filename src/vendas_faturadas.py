import os
print("Rodando em:", os.getcwd())

import re
import ssl
import smtplib
import logging
import traceback
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication


# =========================================================
# CONFIGURAÇÕES GERAIS
# =========================================================

BASE_URL = "https://senffnet.vtexcommercestable.com.br"

VTEX_APP_KEY = os.getenv("VTEX_APP_KEY")
VTEX_APP_TOKEN = os.getenv("VTEX_APP_TOKEN")

SMTP_SERVER = "smtp.skymail.net.br"
SMTP_PORT = 465
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
EMAIL_FROM = SMTP_USER

BASE_OUTPUT_DIR = "output"
CONFIG_SELLERS_FILE = "config/lista_sellers.xlsx"

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "vtex_vendas_faturadas.log")

DEFAULT_MAX_WORKERS = min(32, (os.cpu_count() or 4) * 4)
HTTP_TIMEOUT = 30

TZ_BR = timezone(timedelta(hours=-3))


# =========================================================
# VALIDAÇÕES INICIAIS
# =========================================================

if not all([VTEX_APP_KEY, VTEX_APP_TOKEN, SMTP_USER, SMTP_PASSWORD]):
    raise RuntimeError("❌ Variáveis de ambiente obrigatórias não definidas")


# =========================================================
# LOGS
# =========================================================

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def log(msg: str):
    logging.info(msg)
    print(msg)


# =========================================================
# HELPERS
# =========================================================

def vtex_headers():
    return {
        "Content-Type": "application/json",
        "X-VTEX-API-AppKey": VTEX_APP_KEY,
        "X-VTEX-API-AppToken": VTEX_APP_TOKEN
    }


@lru_cache(maxsize=10000)
def formatar_data_curta(iso_str: str):
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(TZ_BR)
        return dt.strftime("%d/%m/%Y")
    except:
        return iso_str


def br_yesterday_window_to_utc():
    hoje = datetime.now(TZ_BR).date()
    ontem = hoje - timedelta(days=1)

    start = datetime(ontem.year, ontem.month, ontem.day, 0, 0, 0, tzinfo=TZ_BR)
    end   = datetime(ontem.year, ontem.month, ontem.day, 23, 59, 59, 999000, tzinfo=TZ_BR)

    start_utc = start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    end_utc   = end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    return start_utc, end_utc, ontem.strftime("%Y-%m-%d"), ontem.strftime("%d/%m/%Y")


def carregar_sellers_config() -> List[Dict[str, Any]]:
    df = pd.read_excel(CONFIG_SELLERS_FILE)
    sellers = []

    for _, row in df.iterrows():
        if str(row.get("ativo", "")).strip().lower() != "sim":
            continue

        def limpar(raw):
            if pd.isna(raw):
                return []
            return [x.strip() for x in str(raw).split(";") if x.strip()]

        sellers.append({
            "id": str(row["sellerId"]).strip(),
            "display": str(row["sellerName"]).strip(),
            "emailTo": limpar(row.get("emailTo")),
            "emailCc": limpar(row.get("emailCc")),
        })

    log(f"📌 Sellers ativos: {[s['display'] for s in sellers]}")
    return sellers


# =========================================================
# CONSULTAS VTEX
# =========================================================

def listar_pedidos_resumidos_por_seller(start_utc, end_utc, seller_name):
    url = f"{BASE_URL}/api/oms/pvt/orders"
    orders = []
    page = 1

    session = requests.Session()
    session.headers.update(vtex_headers())

    while True:
        params = {
            "page": page,
            "per_page": 100,
            "f_invoicedDate": f"invoicedDate:[{start_utc} TO {end_utc}]",
            "f_status": "invoiced",
            "f_sellerNames": seller_name
        }

        resp = session.get(url, params=params, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            break

        lista = resp.json().get("list", [])
        if not lista:
            break

        orders.extend(lista)
        page += 1

        if len(lista) < 100:
            break

    return orders


def fetch_order_detail(order_id):
    try:
        r = requests.get(
            f"{BASE_URL}/api/oms/pvt/orders/{order_id}",
            headers=vtex_headers(),
            timeout=HTTP_TIMEOUT
        )
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None


# =========================================================
# PROCESSAMENTO
# =========================================================

def get_total_by_id(totals, code):
    for t in totals or []:
        if t.get("id") == code:
            return t.get("value", 0) / 100
    return 0.0


def gerar_linhas_por_seller(order, seller_cfg):
    seller_ids = [s.get("id") for s in order.get("sellers", [])]
    if seller_cfg["id"] not in seller_ids:
        return []

    totals = order.get("totals", [])
    linhas = []

    for tx in order.get("paymentData", {}).get("transactions", []):
        if not tx.get("isActive"):
            continue

        for pm in tx.get("payments", [])[:2]:
            linhas.append({
                "Faturado em": formatar_data_curta(order.get("invoicedDate")),
                "Pedido_Senff": order.get("orderId"),
                "Seller": seller_cfg["display"],
                "Frete": get_total_by_id(totals, "Shipping"),
                "Total_itens": get_total_by_id(totals, "Items"),
                "Valor_total": get_total_by_id(totals, "Shipping") + get_total_by_id(totals, "Items"),
                "Parcelas": pm.get("installments")
            })

    return linhas


# =========================================================
# EMAIL
# =========================================================

def enviar_email(arquivo, seller_cfg, data_brt):
    if not arquivo or not seller_cfg["emailTo"]:
        return

    msg = MIMEMultipart()
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(seller_cfg["emailTo"])
    if seller_cfg["emailCc"]:
        msg["Cc"] = ", ".join(seller_cfg["emailCc"])

    msg["Subject"] = f"Vendas Faturadas – {seller_cfg['display']} – {data_brt}"

    msg.attach(MIMEText(
        f"Segue relatório de vendas faturadas referente a {data_brt}.",
        "plain"
    ))

    with open(arquivo, "rb") as f:
        part = MIMEApplication(f.read(), _subtype="xlsx")
        part.add_header("Content-Disposition", "attachment", filename=os.path.basename(arquivo))
        msg.attach(part)

    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(EMAIL_FROM, seller_cfg["emailTo"] + seller_cfg["emailCc"], msg.as_string())


# =========================================================
# MAIN
# =========================================================

def main():
    try:
        start_utc, end_utc, data_iso, data_brt = br_yesterday_window_to_utc()
        sellers = carregar_sellers_config()

        for seller in sellers:
            log(f"▶ Processando {seller['display']}")

            resumo = listar_pedidos_resumidos_por_seller(start_utc, end_utc, seller["display"])
            if not resumo:
                log("⚠ Sem pedidos")
                continue

            detalhes = {}
            with ThreadPoolExecutor(max_workers=DEFAULT_MAX_WORKERS) as ex:
                futures = {ex.submit(fetch_order_detail, o["orderId"]): o["orderId"] for o in resumo}
                for f in as_completed(futures):
                    if f.result():
                        detalhes[futures[f]] = f.result()

            linhas = []
            for o in resumo:
                if o["orderId"] in detalhes:
                    linhas.extend(gerar_linhas_por_seller(detalhes[o["orderId"]], seller))

            if linhas:
                df = pd.DataFrame(linhas).drop_duplicates()
                path = os.path.join(BASE_OUTPUT_DIR, f"vendas_{data_iso}_{seller['display'].replace(' ', '_')}.xlsx")
                df.to_excel(path, index=False)
                enviar_email(path, seller, data_brt)

        log("✅ Processo finalizado")

    except Exception:
        traceback.print_exc()


if __name__ == "__main__":
    main()
