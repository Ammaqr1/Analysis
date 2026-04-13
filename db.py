import asyncio
import os
import socket
import threading
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import psycopg
from dotenv import load_dotenv
from prisma import Prisma
from prisma.errors import UniqueViolationError
from psycopg.rows import dict_row

load_dotenv(Path(__file__).resolve().parent / ".env")

access_token = os.getenv("access_token")


def _pg_ipv4_hostaddr(hostname: str) -> str | None:
    """
    If hostname is a DNS name, return one IPv4 address for libpq hostaddr.
    EC2 often has no IPv6 route; Supabase and similar hosts may resolve to IPv6 first.
    Skip for literal IPv4/IPv6 (libpq handles those).
    """
    if not hostname:
        return None
    for family in (socket.AF_INET, socket.AF_INET6):
        try:
            socket.inet_pton(family, hostname)
            return None
        except OSError:
            continue
    try:
        infos = socket.getaddrinfo(
            hostname, None, socket.AF_INET, socket.SOCK_STREAM
        )
    except socket.gaierror:
        return None
    if not infos:
        return None
    return infos[0][4][0]


def _pg_connect():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL is not set. Add your Supabase Postgres URL to algtrading/.env "
            "(include ?sslmode=require; URL-encode special characters in the password)."
        )
    prefer_ipv4 = os.getenv("DATABASE_PREFER_IPV4", "true").lower() in (
        "1",
        "true",
        "yes",
    )
    if not prefer_ipv4:
        return psycopg.connect(dsn)
    host = urlparse(dsn).hostname
    hostaddr = _pg_ipv4_hostaddr(host) if host else None
    if hostaddr:
        return psycopg.connect(dsn, hostaddr=hostaddr)
    return psycopg.connect(dsn)


def run_prisma(coro):
    """Run an async Prisma coroutine from sync Flask code."""
    return asyncio.run(coro)


async def _create_auth_user(email: str, password_hash: str, name: str | None):
    db = Prisma()
    await db.connect()
    try:
        return await db.user.create(
            data={
                "email": email,
                "passwordHash": password_hash,
                "name": name,
            }
        )
    finally:
        await db.disconnect()


def create_auth_user(email: str, password_hash: str, name: str | None):
    return run_prisma(_create_auth_user(email, password_hash, name))


async def _find_user_by_email(email: str):
    db = Prisma()
    await db.connect()
    try:
        return await db.user.find_unique(where={"email": email})
    finally:
        await db.disconnect()


def find_user_by_email(email: str):
    return run_prisma(_find_user_by_email(email))

# async def get_user():
    # db = Prisma()
    # await db.connect()
    # found = await db.user.find_many(
    #     where={
    #         "name": "akhil",
    #     }
    # )
    # print(f"Found {len(found)} posts:")
    # print(f"found id:: {found[0].id}")
    # await db.disconnect()
    # return found

def _payload_data_for_order_execution(
    started_time: str,
    at_the_money_time: str,
    at_the_money_price: str,
    exchange: str,
    expiry_date: str,
    ce_ik: str,
    pe_ik: str,
    high_price_ce: str,
    high_price_time_ce: str,
    high_price_pe: str,
    high_price_time_pe: str,
    high_price_start_time: str,
    high_price_end_time: str,
) -> dict:
    return {
        "started_time": started_time,
        "at_the_money_time": at_the_money_time,
        "at_the_money_price": at_the_money_price,
        "exchange": exchange,
        "expiry_date": expiry_date,
        "ce_ik": ce_ik,
        "pe_ik": pe_ik,
        "high_price_ce": high_price_ce,
        "high_price_time_ce": high_price_time_ce,
        "high_price_pe": high_price_pe,
        "high_price_time_pe": high_price_time_pe,
        "high_price_start_time": high_price_start_time,
        "high_price_end_time": high_price_end_time,
    }


async def create_data_for_order_execution(
    started_time: str,
    at_the_money_time: str,
    at_the_money_price: str,
    exchange: str,
    expiry_date: str,
    ce_ik: str,
    pe_ik: str,
    high_price_ce: str,
    high_price_time_ce: str,
    high_price_pe: str,
    high_price_time_pe: str,
    high_price_start_time: str,
    high_price_end_time: str,
) -> dict[str, str]:
    """
    Persist order-execution snapshot. Uses PostgreSQL directly so it does not
    spawn the Prisma query engine (unreliable from background threads near
    loop shutdown). Rows match schema model DataForOrderExecution.
    """
    payload = _payload_data_for_order_execution(
        started_time,
        at_the_money_time,
        at_the_money_price,
        exchange,
        expiry_date,
        ce_ik,
        pe_ik,
        high_price_ce,
        high_price_time_ce,
        high_price_pe,
        high_price_time_pe,
        high_price_start_time,
        high_price_end_time,
    )
    return await asyncio.to_thread(_save_data_for_order_execution_pg, payload)


def create_data_for_order_execution_in_background(**kwargs) -> dict[str, str]:
    """
    Run DB insert on a separate thread so it survives
    event-loop shutdown and does not block strategy flow.

    Writes the same Postgres database Prisma uses; no query-engine subprocess.
    Blocks until the insert completes, then returns the saved row (including id).
    """
    payload = _payload_data_for_order_execution(**kwargs)
    saved: dict[str, str] | None = None
    err: list[BaseException] = []

    def _save_in_thread():
        try:
            nonlocal saved
            saved = _save_data_for_order_execution_pg(payload)
            print("✅ DataForOrderExecution saved")
        except Exception as exc:
            err.append(exc)
            print(f"❌ Background DB save failed: {exc}")

    worker = threading.Thread(target=_save_in_thread, name="db-save-worker")
    worker.start()
    worker.join()
    if err:
        raise err[0]
    assert saved is not None
    return saved


def _save_data_for_order_execution_pg(
    payload: dict, record_id: str | None = None
) -> dict[str, str]:
    now = _now_iso_utc()
    rid = record_id or str(uuid4())
    row = (
        rid,
        now,
        now,
        str(payload["started_time"]),
        str(payload["at_the_money_time"]),
        str(payload["at_the_money_price"]),
        str(payload["exchange"]),
        str(payload["expiry_date"]),
        str(payload["ce_ik"]),
        str(payload["pe_ik"]),
        str(payload["high_price_ce"]),
        str(payload["high_price_time_ce"]),
        str(payload["high_price_pe"]),
        str(payload["high_price_time_pe"]),
        str(payload["high_price_start_time"]),
        str(payload["high_price_end_time"]),
    )
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO "DataForOrderExecution" (
                    "id",
                    "createdAt",
                    "updatedAt",
                    "started_time",
                    "at_the_money_time",
                    "at_the_money_price",
                    "exchange",
                    "expiry_date",
                    "ce_ik",
                    "pe_ik",
                    "high_price_ce",
                    "high_price_time_ce",
                    "high_price_pe",
                    "high_price_time_pe",
                    "high_price_start_time",
                    "high_price_end_time"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                row,
            )
        conn.commit()
    return {
        "id": rid,
        "createdAt": now,
        "updatedAt": now,
        "started_time": str(payload["started_time"]),
        "at_the_money_time": str(payload["at_the_money_time"]),
        "at_the_money_price": str(payload["at_the_money_price"]),
        "exchange": str(payload["exchange"]),
        "expiry_date": str(payload["expiry_date"]),
        "ce_ik": str(payload["ce_ik"]),
        "pe_ik": str(payload["pe_ik"]),
        "high_price_ce": str(payload["high_price_ce"]),
        "high_price_time_ce": str(payload["high_price_time_ce"]),
        "high_price_pe": str(payload["high_price_pe"]),
        "high_price_time_pe": str(payload["high_price_time_pe"]),
        "high_price_start_time": str(payload["high_price_start_time"]),
        "high_price_end_time": str(payload["high_price_end_time"]),
    }


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _payload_buy_order_details(
    o_status: str,
    r_quantity: str,
    r_buy_price: str,
    order_id: str,
    user: str,
    type: str,
    o_buyed_quantity: str,
    m_quantity: str | None = None,
    m_price: str | None = None,
    m_result: str | None = None,
    m_buyed_quantity: str | None = None,
    data_for_order_execution_id: str | None = None,
) -> dict:
    return {
        "user": user,
        "o_status": o_status,
        "r_quantity": r_quantity,
        "r_buy_price": r_buy_price,
        "order_id": order_id,
        "o_buyed_quantity": o_buyed_quantity,
        "m_quantity": m_quantity,
        "type": type,
        "m_price": m_price,
        "m_result": m_result,
        "m_buyed_quantity": m_buyed_quantity,
        "data_for_order_execution_id": data_for_order_execution_id,
    }


def _save_buy_order_details_pg(payload: dict) -> str:
    row_id = str(uuid4())
    now = _now_iso_utc()
    row = (
        row_id,
        now,
        now,
        str(payload["user"]),
        str(payload["type"]),
        str(payload["o_status"]),
        str(payload["r_quantity"]),
        str(payload["r_buy_price"]),
        str(payload["order_id"]),
        str(payload["o_buyed_quantity"]),
        payload["m_quantity"],
        payload["m_price"],
        payload["m_result"],
        payload["m_buyed_quantity"],
        payload.get("data_for_order_execution_id"),
    )
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO "BuyOrderDetails" (
                    "id",
                    "createdAt",
                    "updatedAt",
                    "user",
                    "type",
                    "o_status",
                    "r_quantity",
                    "r_buy_price",
                    "order_id",
                    "o_buyed_quantity",
                    "m_quantity",
                    "m_price",
                    "m_result",
                    "m_buyed_quantity",
                    "dataForOrderExecutionId"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                row,
            )
        conn.commit()
    return row_id


async def create_buy_order_details(
    user: str,
    o_status: str,
    r_quantity: str,
    r_buy_price: str,
    order_id: str,
    o_buyed_quantity: str,
    m_quantity: str | None = None,
    m_price: str | None = None,
    m_result: str | None = None,
    m_buyed_quantity: str | None = None,
    data_for_order_execution_id: str | None = None,
) -> str:
    """Insert a BuyOrderDetails row (PostgreSQL, same DB as Prisma). Returns new row id."""
    payload = _payload_buy_order_details(
        user=user,
        o_status=o_status,
        r_quantity=r_quantity,
        r_buy_price=r_buy_price,
        order_id=order_id,
        o_buyed_quantity=o_buyed_quantity,
        m_quantity=m_quantity,
        m_price=m_price,
        m_result=m_result,
        m_buyed_quantity=m_buyed_quantity,
        data_for_order_execution_id=data_for_order_execution_id,
    )
    return await asyncio.to_thread(_save_buy_order_details_pg, payload)


def create_buy_order_details_in_background(**kwargs):
    """Non-blocking insert; survives event-loop shutdown like other background saves."""
    payload = _payload_buy_order_details(**kwargs)

    def _save_in_thread():
        try:
            _save_buy_order_details_pg(payload)
            print("✅ BuyOrderDetails saved")
        except Exception as exc:
            print(f"❌ BuyOrderDetails background save failed: {exc}")

    worker = threading.Thread(target=_save_in_thread, name="db-buy-order-worker")
    worker.start()
    return worker


def _payload_sell_order_details(
    user: str,
    trade_type: str,
    order_id: str,
    r_quantity: str,
    mode: str,
    stop_loss_price: float,
    stop_loss_trigger_price: float,
    target_price: float,
    target_trigger_price: float,
    o_status: str,
    o_buyed_quantity: float,
    m_quantity: float | None = None,
    m_price: float | None = None,
    m_result: str | None = None,
    m_buyed_quantity: float | None = None,
    data_for_order_execution_id: str | None = None,
) -> dict:
    return {
        "user": user,
        "type": trade_type,
        "order_id": order_id,
        "r_quantity": r_quantity,
        "mode": mode,
        "stop_loss_price": stop_loss_price,
        "stop_loss_trigger_price": stop_loss_trigger_price,
        "target_price": target_price,
        "target_trigger_price": target_trigger_price,
        "o_status": o_status,
        "o_buyed_quantity": o_buyed_quantity,
        "m_quantity": m_quantity,
        "m_price": m_price,
        "m_result": m_result,
        "m_buyed_quantity": m_buyed_quantity,
        "data_for_order_execution_id": data_for_order_execution_id,
    }


def _save_sell_order_details_pg(payload: dict) -> str:
    def _as_optional_float(value):
        if value is None:
            return None
        return float(value)

    row_id = str(uuid4())
    now = _now_iso_utc()
    row = (
        row_id,
        now,
        now,
        str(payload["user"]),
        str(payload["type"]),
        str(payload["order_id"]),
        str(payload["r_quantity"]),
        str(payload["mode"]),
        float(payload["stop_loss_price"]),
        float(payload["stop_loss_trigger_price"]),
        float(payload["target_price"]),
        float(payload["target_trigger_price"]),
        str(payload["o_status"]),
        float(payload["o_buyed_quantity"]),
        _as_optional_float(payload["m_quantity"]),
        _as_optional_float(payload["m_price"]),
        payload["m_result"],
        _as_optional_float(payload["m_buyed_quantity"]),
        payload.get("data_for_order_execution_id"),
    )
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO "SellOrderDetails" (
                    "id",
                    "createdAt",
                    "updatedAt",
                    "user",
                    "type",
                    "order_id",
                    "r_quantity",
                    "mode",
                    "stop_loss_price",
                    "stop_loss_trigger_price",
                    "target_price",
                    "target_trigger_price",
                    "o_status",
                    "o_buyed_quantity",
                    "m_quantity",
                    "m_price",
                    "m_result",
                    "m_buyed_quantity",
                    "dataForOrderExecutionId"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                row,
            )
        conn.commit()
    return row_id


async def create_sell_order_details(
    user: str,
    trade_type: str,
    order_id: str,
    r_quantity: str,
    mode: str,
    stop_loss_price: float,
    stop_loss_trigger_price: float,
    target_price: float,
    target_trigger_price: float,
    o_status: str,
    o_buyed_quantity: float,
    m_quantity: float | None = None,
    m_price: float | None = None,
    m_result: str | None = None,
    m_buyed_quantity: float | None = None,
    data_for_order_execution_id: str | None = None,
) -> str:
    """Insert a SellOrderDetails row (PostgreSQL, same DB as Prisma). Returns new row id."""
    payload = _payload_sell_order_details(
        user=user,
        trade_type=trade_type,
        order_id=order_id,
        r_quantity=r_quantity,
        mode=mode,
        stop_loss_price=stop_loss_price,
        stop_loss_trigger_price=stop_loss_trigger_price,
        target_price=target_price,
        target_trigger_price=target_trigger_price,
        o_status=o_status,
        o_buyed_quantity=o_buyed_quantity,
        m_quantity=m_quantity,
        m_price=m_price,
        m_result=m_result,
        m_buyed_quantity=m_buyed_quantity,
        data_for_order_execution_id=data_for_order_execution_id,
    )
    return await asyncio.to_thread(_save_sell_order_details_pg, payload)


def create_sell_order_details_in_background(**kwargs):
    """Non-blocking insert; survives event-loop shutdown like other background saves."""
    if "trade_type" not in kwargs and "type" in kwargs:
        kwargs = {**kwargs, "trade_type": kwargs.pop("type")}
    payload = _payload_sell_order_details(**kwargs)

    def _save_in_thread():
        try:
            _save_sell_order_details_pg(payload)
            print("✅ SellOrderDetails saved")
        except Exception as exc:
            print(f"❌ SellOrderDetails background save failed: {exc}")

    worker = threading.Thread(target=_save_in_thread, name="db-sell-order-worker")
    worker.start()
    return worker


def _fetch_all_data_for_order_execution() -> list[dict]:
    """Read rows from Postgres (no Prisma query engine)."""
    with _pg_connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                'SELECT * FROM "DataForOrderExecution" ORDER BY "started_time" DESC'
            )
            return [dict(row) for row in cur.fetchall()]


async def get_data_for_order_execution() -> list[dict]:
    return await asyncio.to_thread(_fetch_all_data_for_order_execution)


def _fetch_all_buy_order_details() -> list[dict]:
    """Read BuyOrderDetails rows from Postgres."""
    with _pg_connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute('SELECT * FROM "BuyOrderDetails" ORDER BY "createdAt" DESC')
            return [dict(row) for row in cur.fetchall()]


def _fetch_all_sell_order_details() -> list[dict]:
    """Read SellOrderDetails rows from Postgres."""
    with _pg_connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute('SELECT * FROM "SellOrderDetails" ORDER BY "createdAt" DESC')
            return [dict(row) for row in cur.fetchall()]


async def get_buy_order_details() -> list[dict]:
    return await asyncio.to_thread(_fetch_all_buy_order_details)


async def get_sell_order_details() -> list[dict]:
    return await asyncio.to_thread(_fetch_all_sell_order_details)


def get_buy_order_details_sync() -> list[dict]:
    return _fetch_all_buy_order_details()


def get_sell_order_details_sync() -> list[dict]:
    return _fetch_all_sell_order_details()


def get_data_for_order_execution_sync() -> list[dict]:
    return _fetch_all_data_for_order_execution()

__all__ = [
    "UniqueViolationError",
    "create_auth_user",
    "find_user_by_email",
    "run_prisma",
    "access_token",
    "main",
    "update_access_token",
    "delete_user",
    "get_user",
    "get_buy_order_details",
    "get_sell_order_details",
    "get_data_for_order_execution",
    "get_buy_order_details_sync",
    "get_sell_order_details_sync",
    "get_data_for_order_execution_sync",
]


if __name__ == "__main__":
    async def _demo():
        rows = await get_data_for_order_execution()
        print(rows, "this is the data")
        await create_sell_order_details(
            trade_type="PAPER_TRADE",
            user="ammar1",
            order_id="123",
            r_quantity="100",
            mode="market",
            stop_loss_price=100.235,
            stop_loss_trigger_price=100.521115451515,
            target_price=100.52111545,
            target_trigger_price=100.52111545,
            o_status="COMPLETED",
            o_buyed_quantity=100.52111545,
            m_quantity=100.52111545,
            m_price=100.52111545,
            m_result="COMPLETED",
            m_buyed_quantity=100.52111545,
        )
        print("sell order details created")

    asyncio.run(_demo())
    
