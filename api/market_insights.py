import os
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import FileResponse
from psycopg2.pool import SimpleConnectionPool
from prometheus_fastapi_instrumentator import Instrumentator
from reportlab.pdfgen import canvas
from pathlib import Path

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PDF_PATH = os.path.join(
    os.getenv("STATIC_DIR", BASE_DIR + "/static"),
    os.getenv("REPORT_FILE", "extracted_text.pdf"),
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "marketdb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

app = FastAPI(title="Market Insights API")

POOL = SimpleConnectionPool(
    1,
    10,
    host=POSTGRES_HOST,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    port=POSTGRES_PORT,
)

SQL_SELECT_LATEST = """
SELECT symbol, last_price, moving_avg_1min, vol_1min, updated_at
FROM symbol_metrics
WHERE symbol = %s
LIMIT 1;
"""


def get_conn():
    return POOL.getconn()


def put_conn(conn):
    POOL.putconn(conn)


def get_cursor():
    conn = POOL.getconn()
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()
        POOL.putconn(conn)


@app.on_event("startup")
def startup():
    # instrumentation
    Instrumentator().instrument(app).expose(app)
    # ensure PDF exists (create small placeholder if missing)
    pdf_path = Path(PDF_PATH)
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    if not pdf_path.exists() or pdf_path.suffix.lower() != ".pdf":
        try:
            c = canvas.Canvas(str(pdf_path))
            c.drawString(100, 750, "MarketPulse - Placeholder Report")
            c.drawString(100, 730, "This is an auto-generated placeholder PDF.")
            c.save()
        except Exception:
            # if reportlab not available or fails, just skip - endpoint will still raise 404
            pass


@app.on_event("shutdown")
def shutdown():
    if POOL:
        POOL.closeall()


@app.get("/latest/{symbol}")
def latest_symbol(symbol: str, cur=Depends(get_cursor)):
    cur.execute(SQL_SELECT_LATEST, (symbol.upper(),))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return {
        "symbol": row[0],
        "last_price": float(row[1]) if row[1] is not None else None,
        "moving_avg_1min": float(row[2]) if row[2] is not None else None,
        "volume_1min": int(row[3]) if row[3] is not None else None,
        "updated_at": row[4].isoformat() if row[4] is not None else None,
    }


@app.get("/health")
def health():
    return {"status": "running"}


@app.get("/report")
def get_report():
    if not os.path.isfile(PDF_PATH):
        raise HTTPException(status_code=404, detail="Report not found")
    return FileResponse(
        PDF_PATH, media_type="application/pdf", filename=os.path.basename(PDF_PATH)
    )
