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

import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication


# =========================================================
# CONFIGURAÇÕES GERAIS
# =========================================================

BASE_URL = "https://senffnet.vtexcommercestable.com.br"

#Credenciais do ambiente:
VTEX_APP_KEY = os.getenv("VTEX_APP_KEY")
VTEX_APP_TOKEN = os.getenv("VTEX_APP_TOKEN") 

BASE_OUTPUT_DIR = "output"
CONFIG_SELLERS_FILE = "config/lista_sellers.xlsx"

SMTP_SERVER = "smtp.skymail.net.br"
SMTP_PORT = 465
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
EMAIL_FROM = SMTP_USER

DEFAULT_MAX_WORKERS = min(32, (os.cpu_count() or 4) * 4)
HTTP_TIMEOUT = 30

LOG_FILE = r"D:/BI - SENFF SHOPPING/Relatorios_API-VTEX/VENDAS_FATURADAS/vtex_vendas_faturadas.log"


# =========================================================
# LOGS
# =========================================================

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
        iso_norm = iso_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso_norm).astimezone(timezone(timedelta(hours=-3)))
        return dt.strftime("%d/%m/%y")
    except:
        return iso_str


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

    log(f"📌 Sellers carregados: {[s['display'] for s in sellers]}")
    return sellers


def br_yesterday_window_to_utc():
    tz = timezone(timedelta(hours=-3))
    hoje = datetime.now(tz).date()
    ontem = hoje - timedelta(days=1)

    start = datetime(ontem.year, ontem.month, ontem.day, 0, 0, 0, tzinfo=tz)
    end   = datetime(ontem.year, ontem.month, ontem.day, 23, 59, 59, 999000, tzinfo=tz)

    start_utc = start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    end_utc   = end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    return start_utc, end_utc, ontem.strftime("%Y-%m-%d"), ontem.strftime("%d/%m/%Y")


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
    url = f"{BASE_URL}/api/oms/pvt/orders/{order_id}"
    try:
        resp = requests.get(url, headers=vtex_headers(), timeout=HTTP_TIMEOUT)
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return {}


def buscar_detalhes_pedidos(orders):
    if not orders:
        return {}

    detalhes = {}
    max_workers = min(DEFAULT_MAX_WORKERS, len(orders))

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_order_detail, o["orderId"]): o["orderId"] for o in orders}

        for f in as_completed(futures):
            oid = futures[f]
            try:
                det = f.result()
                if det:
                    detalhes[oid] = det
            except:
                pass

    return detalhes


# =========================================================
# TRANSFORMAÇÕES
# =========================================================

def get_total_by_id(totals, code):
    for t in totals or []:
        if t.get("id") == code:
            return (t.get("value", 0) / 100.0)
    return 0.0


def extrair_pedido_seller(seller_order_id):
    if not seller_order_id:
        return ""
    partes = seller_order_id.split("-")
    if len(partes) < 2:
        return ""
    bloco = re.sub(r"\D", "", partes[-2])
    return bloco[-7:] if len(bloco) >= 7 else ""


def gerar_linhas_por_seller(order, seller_cfg):
    linhas = []
    sellers = order.get("sellers", [])
    seller_ids = [s.get("id") for s in sellers]

    if seller_cfg["id"] not in seller_ids:
        return []

    totals = order.get("totals", [])
    total_itens = get_total_by_id(totals, "Items")
    frete = get_total_by_id(totals, "Shipping")

    pagamento_linhas = []

    for tx in order.get("paymentData", {}).get("transactions", []):
        if tx.get("isActive"):
            pagamento_linhas.extend(tx.get("payments", []))

    if not pagamento_linhas:
        pagamento_linhas = [None]

    linhas_final = []

    for pm in pagamento_linhas[:2]:
        parcelas = pm.get("installments") if pm else None
        linhas_final.append({
            "Faturado em": formatar_data_curta(order.get("invoicedDate")),
            "Pedido_Senff": order.get("orderId"),
            "Pedido_Seller": extrair_pedido_seller(order.get("sellerOrderId", "")),
            "Status": order.get("statusDescription"),
            "Seller": seller_cfg["display"],
            "Frete": frete,
            "Total_itens": total_itens,
            "Valor_total": frete + total_itens,
            "Parcelas": parcelas
        })

    return linhas_final


# =========================================================
# XLSX
# =========================================================

def salvar_xlsx(seller_cfg, linhas, data_iso):
    if not linhas:
        return None

    df = pd.DataFrame(linhas)

    # 🔥 AQUI: Remoção de duplicatas ANTES de salvar
    df = df.drop_duplicates()

    df.sort_values(by=["Pedido_Senff", "Parcelas"], inplace=True)

    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
    filename = f"vendas_{data_iso}_{seller_cfg['display'].replace(' ', '_')}.xlsx"
    path = os.path.join(BASE_OUTPUT_DIR, filename)

    df.to_excel(path, index=False)
    return path


# =========================================================
# EMAIL
# =========================================================

def enviar_email(arquivo, seller_cfg, data_brt):

    if not arquivo:
        return

    to_list = seller_cfg["emailTo"]
    cc_list = seller_cfg["emailCc"]

    if not to_list:
        log(f"⚠ Seller {seller_cfg['display']} não tem emailTo configurado.")
        return

    msg = MIMEMultipart()
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)

    msg["Subject"] = f"Vendas Faturadas – {seller_cfg['display']} – {data_brt}"

    texto = f"""
Olá,

Segue em anexo o relatório de vendas faturadas no Senff Shopping referente ao dia {data_brt}.

Atenciosamente,
Equipe Senff
"""
    msg.attach(MIMEText(texto, "plain"))

    with open(arquivo, "rb") as f:
        part = MIMEApplication(f.read(), _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    part.add_header("Content-Disposition", "attachment", filename=os.path.basename(arquivo))
    msg.attach(part)

    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(SMTP_USER, SMTP_PASSWORD)
            recipients = to_list + cc_list
            server.sendmail(EMAIL_FROM, recipients, msg.as_string())

    except Exception as e:
        log(f"❌ Erro envio email: {e}")


# =========================================================
# MAIN
# =========================================================

def main():
    try:
        start_utc, end_utc, data_iso, data_brt = br_yesterday_window_to_utc()
        sellers = carregar_sellers_config()

        for seller_cfg in sellers:
            log(f"\n===== PROCESSANDO: {seller_cfg['display']} =====")

            summary = listar_pedidos_resumidos_por_seller(start_utc, end_utc, seller_cfg["display"])
            if not summary:
                log(f"⚠ Sem pedidos. Pulando...")
                continue

            detalhes = buscar_detalhes_pedidos(summary)

            linhas = []
            for o in summary:
                oid = o.get("orderId")
                if oid in detalhes:
                    linhas.extend(gerar_linhas_por_seller(detalhes[oid], seller_cfg))

            arquivo = salvar_xlsx(seller_cfg, linhas, data_iso)
            enviar_email(arquivo, seller_cfg, data_brt)

        log("✅ Processo finalizado.")

    except Exception:
        traceback.print_exc()


if __name__ == "__main__":
    main()
