# ============================================================
# api/main.py
# Gateway kryesor — lidh të 4 API-t
# FastAPI + Swagger UI
# ============================================================

import logging
import sys
import os
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.routers import sales, inventory, logistics, procurement

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("main")

# ============================================================
# FASTAPI APP
# ============================================================
app = FastAPI(
    title="Supply Chain Management API",
    description="""
## 🏪 Supply Chain Management API

API i plotë për simulimin e një rrjeti supermarketesh në Shqipëri.

### 4 API të ndara:
- **Sales API** — Transaksione, agregime, KPI shitjesh
- **Inventory API** — Stoku, lëvizjet, alerts
- **Logistics API** — Transport, magazina, rrugë
- **Procurement API** — Porosi furnizuesish, marketing

### Authentication:
Të gjitha endpoints kërkojnë `X-API-Key` header.

### Burime:
- 15 Supermarkete në 8 qytete shqiptare
- 500 produkte × 8 kategori
- Simulim real-time 06:00-22:00
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ============================================================
# CORS
# ============================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# GLOBAL ERROR HANDLER
# ============================================================
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"❌ Unhandled exception: {exc} | Path: {request.url.path}")
    return JSONResponse(
        status_code=500,
        content={
            "status":  "error",
            "message": "Internal server error",
            "path":    str(request.url.path),
        }
    )

# ============================================================
# INCLUDE ROUTERS
# ============================================================
app.include_router(sales.router)
app.include_router(inventory.router)
app.include_router(logistics.router)
app.include_router(procurement.router)

logger.info("✅ Të 4 routers u ngarkuan")

# ============================================================
# ROOT ENDPOINT
# ============================================================
@app.get("/", tags=["Health"])
async def root():
    """Health check i API-t."""
    return {
        "status":    "ok",
        "name":      "Supply Chain Management API",
        "version":   "1.0.0",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "sales":       "/api/v1/sales",
            "inventory":   "/api/v1/inventory",
            "logistics":   "/api/v1/logistics",
            "procurement": "/api/v1/procurement",
            "docs":        "/docs",
            "redoc":       "/redoc",
        }
    }

# ============================================================
# HEALTH CHECK
# ============================================================
@app.get("/health", tags=["Health"])
async def health():
    """Status i detajuar i sistemit."""
    try:
        from config.settings import supabase
        # Test lidhja me Supabase
        response = supabase.table("simulation_config").select("id").limit(1).execute()
        db_status = "ok" if response.data is not None else "error"
    except Exception as e:
        db_status = f"error: {str(e)}"

    return {
        "status":    "ok",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "api":      "ok",
            "database": db_status,
        },
        "simulation": {
            "stores":   15,
            "products": 500,
            "hours":    "06:00-22:00",
        }
    }

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 Duke startuar Supply Chain API...")
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )