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
# from prisma import Prisma
# from prisma.errors import UniqueViolationError
from psycopg.rows import dict_row
from cryptography.fernet import Fernet

load_dotenv(Path(__file__).resolve().parent / ".env")

access_token = os.getenv("access_token")
FERNET_KEY = os.getenv("FERNET_KEY")


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


def _fetch_all_credentials() -> list[dict]:
    """Read Credentials rows from Postgres."""
    with _pg_connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute('SELECT * FROM "Credentials" ORDER BY "createdAt" DESC')
            return [dict(row) for row in cur.fetchall()]


def get_credentials_sync() -> list[dict]:
    return _fetch_all_credentials()


def _ensure_strategy_table_pg(conn) -> None:
    """Create Strategy table if Prisma migration has not created it yet."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS "Strategy" (
                "id" TEXT PRIMARY KEY,
                "createdAt" TIMESTAMP NOT NULL,
                "updatedAt" TIMESTAMP NOT NULL,
                "strategy_name" TEXT,
                "week_day" TEXT,
                "at_time_money" TEXT,
                "start_time" TEXT,
                "end_time" TEXT,
                "end_entry_time" TEXT,
                "exchange" TEXT,
                "target_price" TEXT,
                "stop_loss_price" TEXT,
                "target_trigger_price_percentage" TEXT,
                "stoploss_trigger_price_percentage" TEXT
            )
            """
        )
    conn.commit()


def _fetch_all_strategy_names() -> list[str]:
    """Read strategy names from either Strategy or legacy Stratergy table."""
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            # New schema
            try:
                cur.execute(
                    """
                    SELECT "strategy_name"
                    FROM "Strategy"
                    WHERE "strategy_name" IS NOT NULL AND TRIM("strategy_name") <> ''
                    GROUP BY "strategy_name"
                    ORDER BY MAX("createdAt") DESC
                    """
                )
                return [row[0] for row in cur.fetchall()]
            except psycopg.errors.UndefinedTable:
                conn.rollback()

            # Legacy schema (typo kept for backward compatibility)
            try:
                cur.execute(
                    """
                    SELECT "statergy_name"
                    FROM "Stratergy"
                    WHERE "statergy_name" IS NOT NULL AND TRIM("statergy_name") <> ''
                    GROUP BY "statergy_name"
                    ORDER BY MAX("createdAt") DESC
                    """
                )
                return [row[0] for row in cur.fetchall()]
            except psycopg.errors.UndefinedTable:
                conn.rollback()

        _ensure_strategy_table_pg(conn)
        return []


def get_strategy_names_sync() -> list[str]:
    return _fetch_all_strategy_names()


STRATEGY_MUTABLE_COLUMNS = [
    "strategy_name",
    "week_day",
    "at_time_money",
    "start_time",
    "end_time",
    "end_entry_time",
    "exchange",
    "target_price",
    "stop_loss_price",
    "target_trigger_price_percentage",
    "stoploss_trigger_price_percentage",
]

STRATEGY_SELECT_COLUMNS = ["id", "createdAt", "updatedAt", *STRATEGY_MUTABLE_COLUMNS]


def _strategy_payload(values: dict) -> dict:
    return {
        column: (str(values.get(column, "")).strip() or None)
        for column in STRATEGY_MUTABLE_COLUMNS
    }


def _fetch_strategy_rows_pg(strategy_name: str | None = None) -> list[dict]:
    with _pg_connect() as conn:
        _ensure_strategy_table_pg(conn)
        with conn.cursor(row_factory=dict_row) as cur:
            columns_sql = ", ".join(f'"{column}"' for column in STRATEGY_SELECT_COLUMNS)
            if strategy_name:
                cur.execute(
                    f"""
                    SELECT {columns_sql}
                    FROM "Strategy"
                    WHERE "strategy_name" = %s
                    ORDER BY "createdAt" DESC
                    """,
                    (strategy_name,),
                )
            else:
                cur.execute(
                    f"""
                    SELECT {columns_sql}
                    FROM "Strategy"
                    ORDER BY "createdAt" DESC
                    """
                )
            return [dict(row) for row in cur.fetchall()]


def get_strategy_rows_sync(strategy_name: str | None = None) -> list[dict]:
    return _fetch_strategy_rows_pg(strategy_name)


def _save_strategy_name_pg(strategy_name: str) -> dict | None:
    row_id = str(uuid4())
    created_at = datetime.now()
    updated_at = datetime.now()
    with _pg_connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # New schema
            try:
                cur.execute(
                    """
                    INSERT INTO "Strategy" ("id", "strategy_name", "createdAt", "updatedAt")
                    VALUES (%s, %s, %s, %s)
                    RETURNING *
                    """,
                    (row_id, strategy_name, created_at, updated_at),
                )
                row = cur.fetchone()
                conn.commit()
                return dict(row) if row else None
            except psycopg.errors.UndefinedTable:
                conn.rollback()

            # Legacy schema (typo kept for backward compatibility)
            try:
                cur.execute(
                    """
                    INSERT INTO "Stratergy" ("id", "statergy_name", "createdAt", "updatedAt")
                    VALUES (%s, %s, %s, %s)
                    RETURNING *
                    """,
                    (row_id, strategy_name, created_at, updated_at),
                )
                row = cur.fetchone()
                conn.commit()
                return dict(row) if row else None
            except psycopg.errors.UndefinedTable:
                conn.rollback()

        _ensure_strategy_table_pg(conn)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO "Strategy" ("id", "strategy_name", "createdAt", "updatedAt")
                VALUES (%s, %s, %s, %s)
                RETURNING *
                """,
                (row_id, strategy_name, created_at, updated_at),
            )
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None


async def save_strategy_name(strategy_name: str) -> dict | None:
    strategy_name = strategy_name.strip()
    if not strategy_name:
        raise ValueError("strategy_name is required")
    return await asyncio.to_thread(_save_strategy_name_pg, strategy_name)


def _create_strategy_pg(values: dict) -> dict | None:
    payload = _strategy_payload(values)
    if not payload["strategy_name"]:
        raise ValueError("strategy_name is required")

    row_id = str(uuid4())
    now = datetime.now()
    with _pg_connect() as conn:
        _ensure_strategy_table_pg(conn)
        with conn.cursor(row_factory=dict_row) as cur:
            columns = ["id", "createdAt", "updatedAt", *STRATEGY_MUTABLE_COLUMNS]
            placeholders = ", ".join(["%s"] * len(columns))
            columns_sql = ", ".join(f'"{column}"' for column in columns)
            values_tuple = (
                row_id,
                now,
                now,
                *(payload[column] for column in STRATEGY_MUTABLE_COLUMNS),
            )
            cur.execute(
                f"""
                INSERT INTO "Strategy" ({columns_sql})
                VALUES ({placeholders})
                RETURNING *
                """,
                values_tuple,
            )
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None


async def create_strategy(values: dict) -> dict | None:
    return await asyncio.to_thread(_create_strategy_pg, values)


def _update_strategy_pg(strategy_id: str, values: dict) -> dict | None:
    if not strategy_id:
        raise ValueError("strategy id is required")

    payload = _strategy_payload(values)
    if not payload["strategy_name"]:
        raise ValueError("strategy_name is required")

    now = datetime.now()
    set_sql = ", ".join(f'"{column}" = %s' for column in STRATEGY_MUTABLE_COLUMNS)
    with _pg_connect() as conn:
        _ensure_strategy_table_pg(conn)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                UPDATE "Strategy"
                SET {set_sql}, "updatedAt" = %s
                WHERE "id" = %s
                RETURNING *
                """,
                (
                    *(payload[column] for column in STRATEGY_MUTABLE_COLUMNS),
                    now,
                    strategy_id,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None


async def update_strategy(strategy_id: str, values: dict) -> dict | None:
    return await asyncio.to_thread(_update_strategy_pg, strategy_id, values)


def _delete_strategy_pg(strategy_id: str) -> dict | None:
    if not strategy_id:
        raise ValueError("strategy id is required")

    with _pg_connect() as conn:
        _ensure_strategy_table_pg(conn)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                DELETE FROM "Strategy"
                WHERE "id" = %s
                RETURNING *
                """,
                (strategy_id,),
            )
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None


async def delete_strategy(strategy_id: str) -> dict | None:
    return await asyncio.to_thread(_delete_strategy_pg, strategy_id)


async def save_user_credentials(
    user: str, api_key: str, api_secrets: str, phone_no: str, totp_bar_code: str, pin_code: str
) -> dict | None:
    if not api_key:
        raise ValueError("api_key is required")
    if not api_secrets:
        raise ValueError("api_secrets is required")
    if not phone_no:
        raise ValueError("phone_no is required")
    if not totp_bar_code:
        raise ValueError("totp_bar_code is required")
    if not pin_code:
        raise ValueError("pin_code is required")
    if not user:
        raise ValueError("user is required")
    return await asyncio.to_thread(
        _save_user_credentials_pg, user, api_key, api_secrets, phone_no, totp_bar_code, pin_code
    )



def _save_user_credentials_pg(
    user: str, api_key: str, api_secrets: str, phone_no: str, totp_bar_code: str, pin_code: str
) -> dict | None:
    user_id = str(uuid4())
    print(f"User ID: {user_id}")
    createdAt = datetime.now()
    updatedAt = datetime.now()
    mode = "off"
    with _pg_connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO "Credentials" ("id", "user", "api_key", "api_secrets", "phone_no", "totp_bar_code", "pin_code","mode", "createdAt", "updatedAt")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                RETURNING *
                """,
                (user_id, user, api_key, api_secrets, phone_no, totp_bar_code, pin_code, mode, createdAt, updatedAt),
            )
            row = cur.fetchone()
            conn.commit()
            print(f"User credentials saved: {row}")
            return dict(row) if row else None



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
    "get_credentials_sync",
    "get_strategy_names_sync",
    "get_strategy_rows_sync",
    "save_strategy_name",
    "create_strategy",
    "update_strategy",
    "delete_strategy",
]


def encrypt_text(plain_text: str) -> str:
    """
    Encrypt UTF-8 text with Fernet and return token as ASCII string.
    """
    try:
        token = Fernet(FERNET_KEY.encode("ascii")).encrypt(plain_text.encode("utf-8"))
        return token.decode("ascii")
    except Exception as exc:
        raise ValueError(f"Failed to encrypt payload: {exc}") from exc


if __name__ == "__main__":
    async def _demo():
        await save_user_credentials(
            user="ammar1",
            api_key="12345",
            api_secrets="12345",
            phone_no="12345",
            totp_bar_code="12345",
            pin_code="12345",
        )
        # rows = await get_data_for_order_execution()
        # print(rows, "this is the data")
        # await create_sell_order_details(
        #     trade_type="PAPER_TRADE",
        #     user="ammar1",
        #     order_id="123",
        #     r_quantity="100",
        #     mode="market",
        #     stop_loss_price=100.235,
        #     stop_loss_trigger_price=100.521115451515,
        #     target_price=100.52111545,
        #     target_trigger_price=100.52111545,
        #     o_status="COMPLETED",
        #     o_buyed_quantity=100.52111545,
        #     m_quantity=100.52111545,
        #     m_price=100.52111545,
        #     m_result="COMPLETED",
        #     m_buyed_quantity=100.52111545,
        # )
        # print("sell order details created")

    asyncio.run(_demo())



