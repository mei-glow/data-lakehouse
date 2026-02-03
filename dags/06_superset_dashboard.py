from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import timedelta
import pendulum
import logging
import json
import time
import requests

# =============================================================================
# CONFIG
# =============================================================================
SUPERSET_URL  = "http://superset:8088"
SUPERSET_USER = "admin"
SUPERSET_PASS = "admin"

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_DB   = "lakehouse"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASS = "clickhouse123"
CLICKHOUSE_HTTP_PORT = 8123

DB_NAME   = "ClickHouse — Lakehouse"
DASH_NAME = "E-Commerce Lakehouse Analytics"
DASH_SLUG = "ecommerce-lakehouse-analytics"

CACHE_TIMEOUT = 300

logger = logging.getLogger(__name__)

# =============================================================================
# Superset API Client
# =============================================================================
class SupersetClient:
    def __init__(self):
        self.session = requests.Session()
        self.jwt = None
        self.csrf = None
        self._login()
        self._csrf()

    def _login(self):
        r = self.session.post(
            f"{SUPERSET_URL}/api/v1/security/login",
            json={"username": SUPERSET_USER, "password": SUPERSET_PASS, "provider": "db"},
            timeout=30,
        )
        if r.status_code != 200:
            raise RuntimeError(f"JWT login failed: {r.status_code} {r.text}")
        data = r.json()
        if "access_token" not in data:
            raise RuntimeError(f"JWT login missing access_token: {r.text}")
        self.jwt = data["access_token"]
        logger.info("✅ JWT login OK")

    def _csrf(self):
        r = self.session.get(
            f"{SUPERSET_URL}/api/v1/security/csrf_token/",
            headers={"Authorization": f"Bearer {self.jwt}"},
            timeout=30,
        )
        if r.status_code != 200:
            raise RuntimeError(f"CSRF token failed: {r.status_code} {r.text}")
        self.csrf = r.json()["result"]
        logger.info("✅ CSRF OK")

    def api(self, method, path, body=None, timeout=60):
        headers = {
            "Authorization": f"Bearer {self.jwt}",
            "Content-Type": "application/json",
        }
        if method.upper() in ("POST", "PUT", "DELETE"):
            headers["X-CSRFToken"] = self.csrf

        return self.session.request(
            method.upper(),
            f"{SUPERSET_URL}{path}",
            headers=headers,
            json=body,
            timeout=timeout,
        )


# =============================================================================
# DATASETS — atomic, đúng schema ClickHouse bạn đưa
# =============================================================================
DATASETS = [
    {
        "key": "daily_sales",
        "name": "Daily Sales Atomic",
        "sql": """
SELECT
  sale_date,
  total_revenue,
  total_orders,
  avg_order_value,
  conversion_rate,
  cart_abandonment_rate,
  total_carts
FROM lakehouse.daily_sales_summary
""".strip(),
    },
    {
        "key": "funnel_daily",
        "name": "Conversion Funnel Atomic",
        "sql": """
SELECT
  analysis_date,
  stage_1_view_users,
  stage_2_cart_users,
  stage_3_purchase_users
FROM lakehouse.conversion_funnel_daily
""".strip(),
    },
    {
        "key": "product_perf",
        "name": "Product Performance Atomic",
        "sql": """
SELECT
  product_id,
  brand,
  category_level_1,
  total_revenue
FROM lakehouse.product_performance
""".strip(),
    },
    {
        "key": "rfm",
        "name": "RFM Atomic",
        "sql": """
SELECT
  user_id,
  rfm_segment,
  monetary_value
FROM lakehouse.user_rfm_segments
WHERE rfm_segment IS NOT NULL
""".strip(),
    },
    {
        "key": "hourly",
        "name": "Hourly Traffic Atomic",
        "sql": """
SELECT
  event_hour,
  day_of_week,
  total_events
FROM lakehouse.hourly_traffic
""".strip(),
    },
    {
        "key": "category_perf",
        "name": "Category Performance Atomic",
        "sql": """
SELECT
  category_level_1,
  category_level_2,
  total_revenue
FROM lakehouse.category_performance
WHERE category_level_1 IS NOT NULL
""".strip(),
    },
]


# =============================================================================
# CHARTS — dùng SQLExpression metrics để chắc chắn render
# Notes:
# - Superset mỗi viz plugin có control keys riêng, nhưng bộ key tối thiểu thường gồm:
#   line: x_axis (time col) + metrics
#   bar: groupby + metrics (+ order_by_cols)
#   pie: groupby + metric
#   pivot_table: rows + columns + metrics
#   treemap: groupby(hierarchy) + metrics
# - Mình set thêm fallback keys (x, y...) để tránh "Add required control values"
# =============================================================================
def sql_metric(label: str, expr: str):
    return {"label": label, "expressionType": "SQL", "sqlExpression": expr}

CHARTS = [
    {
        "name": "📈 Daily Revenue & Orders",
        "dataset_key": "daily_sales",
        "viz_type": "line",
        "params": {
            # primary keys (newer superset)
            "x_axis": "sale_date",
            "metrics": [
                sql_metric("Revenue", "SUM(total_revenue)"),
                sql_metric("Orders", "SUM(total_orders)"),
            ],
            # fallback keys (some plugins)
            "x": "sale_date",
            "y": ["SUM(total_revenue)", "SUM(total_orders)"],
            "row_limit": 1000,
            "show_legend": True,
            "time_grain_sqla": "P1D",
        },
    },
    {
        "name": "🎯 Conversion Funnel (Users)",
        "dataset_key": "funnel_daily",
        "viz_type": "bar",
        "params": {
            "groupby": ["analysis_date"],
            "metrics": [
                sql_metric("Views", "SUM(stage_1_view_users)"),
                sql_metric("Carts", "SUM(stage_2_cart_users)"),
                sql_metric("Purchases", "SUM(stage_3_purchase_users)"),
            ],
            "stacked": True,
            "row_limit": 60,
            "order_by_cols": ["analysis_date ASC"],
            # fallback
            "x": "analysis_date",
        },
    },
    {
        "name": "🏆 Top 10 Products by Revenue",
        "dataset_key": "product_perf",
        "viz_type": "bar",
        "params": {
            "groupby": ["product_id"],
            "metrics": [sql_metric("Revenue", "SUM(total_revenue)")],
            "row_limit": 10,
            "order_desc": True,
            "order_by_cols": ["SUM(total_revenue) DESC"],
            # fallback
            "x": "product_id",
        },
    },
    {
        "name": "👥 RFM Segments (Customer Count)",
        "dataset_key": "rfm",
        "viz_type": "pie",
        "params": {
            "groupby": ["rfm_segment"],
            "metric": sql_metric("Customers", "COUNT(user_id)"),
            "show_labels": True,
            "show_percentage": True,
            "row_limit": 50,
        },
    },
    {
        "name": "⏰ Hourly Traffic Heatmap",
        "dataset_key": "hourly",
        "viz_type": "pivot_table",
        "params": {
            "rows": ["event_hour"],
            "columns": ["day_of_week"],
            "metrics": [sql_metric("Events", "SUM(total_events)")],
            "row_limit": 500,
        },
    },
    {
        "name": "📊 Category Revenue Treemap",
        "dataset_key": "category_perf",
        "viz_type": "treemap",
        "params": {
            "groupby": ["category_level_1", "category_level_2"],
            "metrics": [sql_metric("Revenue", "SUM(total_revenue)")],
            "row_limit": 15,
        },
    },
    {
        "name": "🛒 Abandonment vs Conversion Rate",
        "dataset_key": "daily_sales",
        "viz_type": "line",
        "params": {
            "x_axis": "sale_date",
            "metrics": [
                sql_metric("Abandonment", "AVG(cart_abandonment_rate)"),
                sql_metric("Conversion",  "AVG(conversion_rate)"),
            ],
            "x": "sale_date",
            "row_limit": 1000,
            "show_legend": True,
            "time_grain_sqla": "P1D",
        },
    },
]


# =============================================================================
# HELPERS — idempotent CRUD
# =============================================================================
def check_superset_health(**_):
    logger.info("🔍 Waiting for Superset...")
    for i in range(1, 121):
        try:
            r = requests.get(f"{SUPERSET_URL}/superset/welcome/", timeout=5, allow_redirects=False)
            if r.status_code in (200, 302):
                logger.info("✅ Superset healthy")
                return
            logger.info("  [%d/120] status=%d", i, r.status_code)
        except Exception as e:
            logger.info("  [%d/120] %s", i, e)
        time.sleep(2)
    raise RuntimeError("Superset not healthy in time")


def _get_db_id_by_name(client: SupersetClient, name: str):
    r = client.api("GET", "/api/v1/database/")
    if r.status_code != 200:
        raise RuntimeError(f"GET database list failed: {r.status_code} {r.text}")
    for db in r.json().get("result", []):
        if db.get("database_name") == name:
            return db["id"]
    return None


def register_clickhouse_db(**ctx):
    client = SupersetClient()

    # ClickHouse SQLAlchemy URI for clickhouse-connect dialect
    sqlalchemy_uri = (
        f"clickhousedb://{CLICKHOUSE_USER}:{CLICKHOUSE_PASS}"
        f"@{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/{CLICKHOUSE_DB}"
    )

    existing_id = _get_db_id_by_name(client, DB_NAME)
    if existing_id:
        # Update to ensure allow_run_async True
        upd = client.api("PUT", f"/api/v1/database/{existing_id}", {
            "allow_run_async": True,
            "expose_in_sqllab": True,
        })
        logger.info("⏭️ DB exists id=%s | PUT allow_run_async -> %d", existing_id, upd.status_code)
        ctx["ti"].xcom_push(key="db_id", value=existing_id)
        return

    # Test connection
    test_payload = {
        "sqlalchemy_uri": sqlalchemy_uri,
        "configuration_method": "sqlalchemy_form",
        "extra": json.dumps({"engine_specific_config": {}, "connect_args": {}}),
    }
    tr = client.api("POST", "/api/v1/database/test_connection/", test_payload)
    logger.info("🔌 test_connection -> %d %s", tr.status_code, tr.text[:200])
    if tr.status_code != 200:
        raise RuntimeError(f"test_connection failed: {tr.status_code} {tr.text}")

    # Create DB (handle 422 duplicate gracefully)
    payload = {
        "database_name": DB_NAME,
        "sqlalchemy_uri": sqlalchemy_uri,
        "configuration_method": "sqlalchemy_form",
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "extra": json.dumps({"engine_specific_config": {}, "connect_args": {}}),
    }
    cr = client.api("POST", "/api/v1/database/", payload)

    if cr.status_code in (200, 201):
        db_id = cr.json()["id"]
        logger.info("✅ DB created id=%s", db_id)
        ctx["ti"].xcom_push(key="db_id", value=db_id)
        return

    if cr.status_code == 422 and "already exists" in cr.text.lower():
        # Another run created it; fetch id again
        db_id = _get_db_id_by_name(client, DB_NAME)
        if not db_id:
            raise RuntimeError(f"DB duplicate but cannot find id: {cr.text}")
        logger.info("✅ DB duplicate handled, using existing id=%s", db_id)
        ctx["ti"].xcom_push(key="db_id", value=db_id)
        return

    raise RuntimeError(f"Create database failed: {cr.status_code} {cr.text}")


def _find_dataset(client: SupersetClient, table_name: str):
    r = client.api("GET", f"/api/v1/dataset/?filter=table_name eq '{table_name}'")
    if r.status_code != 200:
        return None
    for d in r.json().get("result", []):
        if d.get("table_name") == table_name:
            return d
    return None


def _find_chart(client: SupersetClient, slice_name: str):
    r = client.api("GET", f"/api/v1/chart/?filter=slice_name eq '{slice_name}'")
    if r.status_code != 200:
        return None
    for c in r.json().get("result", []):
        if c.get("slice_name") == slice_name:
            return c
    return None


def create_or_update_datasets_and_charts(**ctx):
    client = SupersetClient()
    db_id = ctx["ti"].xcom_pull(task_ids="register_clickhouse_db", key="db_id")

    ds_ids = {}  # dataset_key -> dataset_id

    # 1) datasets
    for ds in DATASETS:
        existing = _find_dataset(client, ds["name"])
        if existing:
            ds_id = existing["id"]
            # update SQL to ensure latest
            ur = client.api("PUT", f"/api/v1/dataset/{ds_id}", {
                "sql": ds["sql"],
                "normalize_columns": True,
            })
            logger.info("⏭️ dataset exists '%s' id=%s | PUT -> %d", ds["name"], ds_id, ur.status_code)
        else:
            cr = client.api("POST", "/api/v1/dataset/", {
                "database": db_id,
                "table_name": ds["name"],
                "sql": ds["sql"],
                "normalize_columns": True,
            })
            if cr.status_code not in (200, 201):
                raise RuntimeError(f"Create dataset failed '{ds['name']}': {cr.status_code} {cr.text}")
            ds_id = cr.json()["id"]
            logger.info("✅ dataset created '%s' id=%s", ds["name"], ds_id)

        ds_ids[ds["key"]] = ds_id

    # 2) charts
    chart_ids = []
    for ch in CHARTS:
        ds_id = ds_ids[ch["dataset_key"]]

        params = {"viz_type": ch["viz_type"], "datasource": f"{ds_id}__table"}
        params.update(ch["params"])

        existing = _find_chart(client, ch["name"])
        if existing:
            chart_id = existing["id"]
            ur = client.api("PUT", f"/api/v1/chart/{chart_id}", {
                "viz_type": ch["viz_type"],
                "datasource_id": ds_id,
                "datasource_type": "table",
                "params": json.dumps(params),
                "cache_timeout": CACHE_TIMEOUT,
            })
            logger.info("⏭️ chart exists '%s' id=%s | PUT -> %d", ch["name"], chart_id, ur.status_code)
        else:
            cr = client.api("POST", "/api/v1/chart/", {
                "slice_name": ch["name"],
                "viz_type": ch["viz_type"],
                "datasource_id": ds_id,
                "datasource_type": "table",
                "params": json.dumps(params),
                "cache_timeout": CACHE_TIMEOUT,
            })
            if cr.status_code not in (200, 201):
                raise RuntimeError(f"Create chart failed '{ch['name']}': {cr.status_code} {cr.text}")
            chart_id = cr.json()["id"]
            logger.info("✅ chart created '%s' id=%s", ch["name"], chart_id)

        chart_ids.append(chart_id)

    ctx["ti"].xcom_push(key="chart_ids", value=chart_ids)
    logger.info("📦 charts ready: %s", chart_ids)


def _build_position_json(chart_ids):
    # layout đẹp, đúng “story”
    # grid width 36
    # row height ~ 150
    pos = {
        f"chart_{chart_ids[0]}": {"i": f"chart_{chart_ids[0]}", "x": 0,  "y": 0,  "w": 18, "h": 6, "type": "CHART", "id": chart_ids[0]},
        f"chart_{chart_ids[6]}": {"i": f"chart_{chart_ids[6]}", "x": 18, "y": 0,  "w": 18, "h": 6, "type": "CHART", "id": chart_ids[6]},

        f"chart_{chart_ids[1]}": {"i": f"chart_{chart_ids[1]}", "x": 0,  "y": 6,  "w": 36, "h": 7, "type": "CHART", "id": chart_ids[1]},

        f"chart_{chart_ids[2]}": {"i": f"chart_{chart_ids[2]}", "x": 0,  "y": 13, "w": 18, "h": 7, "type": "CHART", "id": chart_ids[2]},
        f"chart_{chart_ids[3]}": {"i": f"chart_{chart_ids[3]}", "x": 18, "y": 13, "w": 18, "h": 7, "type": "CHART", "id": chart_ids[3]},

        f"chart_{chart_ids[4]}": {"i": f"chart_{chart_ids[4]}", "x": 0,  "y": 20, "w": 36, "h": 7, "type": "CHART", "id": chart_ids[4]},
        f"chart_{chart_ids[5]}": {"i": f"chart_{chart_ids[5]}", "x": 0,  "y": 27, "w": 36, "h": 8, "type": "CHART", "id": chart_ids[5]},
    }
    return json.dumps(pos)


def create_or_update_dashboard(**ctx):
    client = SupersetClient()
    chart_ids = ctx["ti"].xcom_pull(task_ids="create_or_update_datasets_and_charts", key="chart_ids")

    # find dashboard by slug
    r = client.api("GET", f"/api/v1/dashboard/?filter=slug eq '{DASH_SLUG}'")
    dash_id = None
    if r.status_code == 200:
        for d in r.json().get("result", []):
            if d.get("slug") == DASH_SLUG:
                dash_id = d["id"]
                break

    payload = {
        "dashboard_title": DASH_NAME,
        "slug": DASH_SLUG,
        "published": True,
        "position_json": _build_position_json(chart_ids),
    }

    if dash_id:
        ur = client.api("PUT", f"/api/v1/dashboard/{dash_id}", payload)
        logger.info("⏭️ dashboard exists id=%s | PUT -> %d", dash_id, ur.status_code)
    else:
        cr = client.api("POST", "/api/v1/dashboard/", payload)
        if cr.status_code not in (200, 201):
            raise RuntimeError(f"Create dashboard failed: {cr.status_code} {cr.text}")
        dash_id = cr.json()["id"]
        logger.info("✅ dashboard created id=%s", dash_id)

    ctx["ti"].xcom_push(key="dashboard_id", value=dash_id)


def warmup_charts(**ctx):
    """
    Warm up chart data so UI won't spin forever.
    Uses /api/v1/chart/data endpoint.
    """
    client = SupersetClient()
    chart_ids = ctx["ti"].xcom_pull(task_ids="create_or_update_datasets_and_charts", key="chart_ids")

    for cid in chart_ids:
        body = {
            "force": True,
            "queries": [{"extras": {}, "filters": [], "columns": [], "metrics": [], "row_limit": 1000}],
            "result_format": "json",
            "result_type": "full",
        }
        r = client.api("POST", "/api/v1/chart/data", body, timeout=120)
        logger.info("🔥 warmup chart id=%s -> %d", cid, r.status_code)


def verify_end_to_end(**ctx):
    client = SupersetClient()
    dash_id = ctx["ti"].xcom_pull(task_ids="create_or_update_dashboard", key="dashboard_id")

    r = client.api("GET", f"/api/v1/dashboard/{dash_id}")
    if r.status_code != 200:
        raise RuntimeError(f"Dashboard GET failed: {r.status_code} {r.text}")

    data = r.json().get("result", {})
    slug = data.get("slug", "?")
    pos = data.get("position_json", "{}")
    try:
        pos_obj = json.loads(pos) if isinstance(pos, str) else pos
    except Exception:
        pos_obj = {}
    charts = sum(1 for v in pos_obj.values() if v.get("type") == "CHART")

    logger.info("✅ verify dashboard slug=%s charts_in_layout=%d", slug, charts)
    if charts < 7:
        logger.warning("⚠️ expected 7 charts, got %d", charts)


def print_access_info(**ctx):
    dash_id = ctx["ti"].xcom_pull(task_ids="create_or_update_dashboard", key="dashboard_id")
    msg = f"""
╔══════════════════════════════════════════════════════════════╗
║          ✅  SUPERSET DASHBOARD READY                       ║
╠══════════════════════════════════════════════════════════════╣
║  🌐 Dashboard URL (host browser):                            ║
║     http://localhost:8089/superset/dashboard/{DASH_SLUG}/    ║
║  🔑 Login: admin / admin                                     ║
║  🆔 Dashboard ID: {dash_id}                                  ║
╚══════════════════════════════════════════════════════════════╝
"""
    logger.info(msg)
    print(msg)


# =============================================================================
# DAG
# =============================================================================
default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="06_superset_dashboard",
    description="Build E-Commerce Lakehouse dashboard on Apache Superset (API-driven)",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lakehouse", "superset", "dashboard", "visualization"],
) as dag:

    start = EmptyOperator(task_id="start")

    t_health  = PythonOperator(task_id="check_superset_health", python_callable=check_superset_health)
    t_db      = PythonOperator(task_id="register_clickhouse_db", python_callable=register_clickhouse_db)
    t_build   = PythonOperator(task_id="create_or_update_datasets_and_charts", python_callable=create_or_update_datasets_and_charts)
    t_dash    = PythonOperator(task_id="create_or_update_dashboard", python_callable=create_or_update_dashboard)
    t_warmup  = PythonOperator(task_id="warmup_charts", python_callable=warmup_charts)
    t_verify  = PythonOperator(task_id="verify_end_to_end", python_callable=verify_end_to_end)
    t_info    = PythonOperator(task_id="print_access_info", python_callable=print_access_info)

    end = EmptyOperator(task_id="end")

    start >> t_health >> t_db >> t_build >> t_dash >> t_warmup >> t_verify >> t_info >> end
