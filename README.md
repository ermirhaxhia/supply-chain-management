# 🏪 Supply Chain Management — Simulation Platform

> **Platformë e plotë simulimi për një rrjet supermarketesh në Shqipëri**  

[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.135-green?logo=fastapi)](https://fastapi.tiangolo.com)
[![Supabase](https://img.shields.io/badge/Supabase-PostgreSQL-3ECF8E?logo=supabase)](https://supabase.com)
[![Render](https://img.shields.io/badge/Deploy-Render-46E3B7?logo=render)](https://render.com)

---

## 📋 Përmbajtja

- [Çfarë është ky projekt](#-çfarë-është-ky-projekt)
- [Pse u krijua](#-pse-u-krijua)
- [Arkitektura e Sistemit](#-arkitektura-e-sistemit)
- [Modulet e Simulimit](#-modulet-e-simulimit)
- [Bazat Matematikore](#-bazat-matematikore)
- [Struktura e Projektit](#-struktura-e-projektit)
- [Databaza](#-databaza)
- [API Endpoints](#-api-endpoints)
- [Teknologjitë e Përdorura](#-teknologjitë-e-përdorura)
- [Njohuritë Akademike](#-njohuritë-akademike)
- [Instalimi](#-instalimi)
- [Konfigurimi](#-konfigurimi)
- [Përdorimi](#-përdorimi)
- [Projeksionet e të Dhënave](#-projeksionet-e-të-dhënave)
- [Black Swan Events](#-black-swan-events)
- [Strategjia e Ruajtjes së Të Dhënave](#-strategjia-e-ruajtjes-së-të-dhënave)

---

## 🎯 Çfarë është ky projekt

**Supply Chain Management Simulation Platform** është një sistem i plotë simulimi që modelon operacionet e një rrjeti prej **15 supermarketesh** në 8 qytete shqiptare. Sistemi gjeneron të dhëna realiste dhe të vazhdueshme çdo orë, nga ora 06:00 deri 22:00, duke mbuluar të gjithë ciklit të zinxhirit të furnizimit:

```
Furnizuesi → Magazina → Supermarketi → Klienti
```

Platforma integron **4 mikroshërbime API**, një **motor simulimi stokastik**, dhe një **sistem agregimi të shtresëzuar** të dhënash — të gjitha të ndërtuara nga zeroja duke aplikuar teori nga matematika e aplikuar.

---

## 💡 Pse u krijua

### Konteksti Akademik
Ky projekt është ndërtuar si **projekt portofoli qendror** për programin MSc në Matematikë të Aplikuar (Inxhinieri Matematike dhe Kompjuterike) në Departamentin e Matematikës së Aplikuar, FSHN, Universiteti i Tiranës.

### Qëllimi Kryesor
Demonstrimi praktik i lëndëve akademike:
- **Kërkime Operacionale** → Optimizimi i rrugëve të transportit, EOQ, LP
- **Optimizim** → Minimizimi i kostove të zinxhirit të furnizimit
- **Probabilitet dhe Proceset e Rastit** → NHPP, Zinxhirët Markov, Monte Carlo

### Objektivat Specifike
1. Ndërtimi i një sistemi simulimi realist të nivelit enterprise
2. Demonstrimi i aftësive Data Engineering / BI Developer
3. Aplikimi i metodave statistikore mbi të dhëna të gjeneruara
4. Krijimi i një API të plotë për konsumim nga Power BI / Excel

---

## 🏗️ Arkitektura e Sistemit

```
┌─────────────────────────────────────────────────────────────┐
│                    SIMULATION ENGINE                         │
│  demand_profile → sales → inventory → purchasing            │
│               → transport → warehouse → marketing           │
└─────────────────────────┬───────────────────────────────────┘
                          │ çdo orë 06:00-22:00
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    SUPABASE (PostgreSQL)                     │
│  22 tabela: Reference | Operative | Aggregate               │
└──────────┬──────────────────────────────────┬───────────────┘
           │ lexon                            │ agregate
           ▼                                 ▼
┌──────────────────────┐         ┌──────────────────────────┐
│   4 FastAPI          │         │   Aggregate Tables        │
│   Microservices      │         │   sales_hourly/daily/     │
│   + Auth (API Keys)  │         │   monthly, kpi_monthly    │
└──────────┬───────────┘         └──────────────────────────┘
           │
           ▼
┌──────────────────────┐   ┌──────────────────────────────┐
│   Power BI           │   │   Streamlit Admin UI          │
│   Dashboard          │   │   Control Panel + Events      │
└──────────────────────┘   └──────────────────────────────┘
```

---

## ⚙️ Modulet e Simulimit

### 1. 📊 Demand Profile (`demand_profile.py`)
Zemra e simulimit — llogarit numrin e klientëve për çdo store dhe orë duke përdorur **Procesin Poisson jo-homogjen (NHPP)**:

```
λᵢ(t) = λᵢ_base × Trend(m) × Weekly(d) × Hourly(h) × Holiday(t) × Event(config) × ε(t)
```

**Vektori i Lambdave** — çdo store ka λ të ndryshme:
| Store | Qytet | λ_final (klientë/orë) |
|-------|-------|----------------------|
| STR-01 | Tiranë Qendër | 218 |
| STR-02 | Tiranë Bllok | 173 |
| STR-06 | Durrës Qendër | 101 |
| STR-08 | Vlorë Qendër | 62 |
| STR-15 | Berat Qendër | 29 |

### 2. 🛒 Sales Module (`sales_module.py`)
Gjeneron transaksione individuale bazuar në **Shpërndarjen e Shportës**:

| Tipi | Probabiliteti | Produkte/Faturë | Avg Lekë |
|------|--------------|-----------------|----------|
| Quick (blerje e shpejtë) | 35% | 1-3 | ~500 |
| Medium | 40% | 3-6 | ~1,200 |
| Family | 20% | 6-12 | ~3,500 |
| Bulk (magazinim) | 5% | 10-25 | ~8,000 |

**Target Revenue:** ~1,300-1,500 Lekë/faturë (≈€12-14) — realist për Shqipëri.

### 3. 📦 Inventory Module (`inventory_module.py`)
- Menaxhon stokun e 500 produkteve × 15 store-eve (7,500 entries)
- Aplikon humbjet nga skadimi (Expired) dhe vjedhja/dëmtimi (Shrinkage)
- Trigger restock automatik kur stock ≤ reorder_point
- Batch INSERT për performancë optimale

### 4. 🛍️ Purchasing Module (`purchasing_module.py`)
- Krijon Purchase Orders automatike me **Quantity Discount**
- Lead Time ~ Poisson(λ) × lead_time_multiplier
- Anti-duplication: nuk krijon porosi nëse ekziston aktive
- Ndikimi i çmimeve nga Black Swan Events (luftë, krizë)

### 5. 🚛 Transport Module (`transport_module.py`)
- Simulon dërgesa 2 herë/ditë (06:00 dhe 14:00)
- Kosto = distance_km × fuel_price × consumption + driver_cost
- Vonesë ~ Poisson me probabilitet 12% (ndryshon me ngjarjet)
- OTD (On-Time Delivery) target: ~80-88%

### 6. 🏭 Warehouse Module (`warehouse_module.py`)
- Snapshot çdo orë për 5 magazina (1 qendrore + 4 rajonale)
- Monitoron: kapacitet, inbound/outbound units, labor hours
- Alert automatik kur kapaciteti > 90% (nga Black Swan)

### 7. 📣 Marketing Module (`marketing_module.py`)
- Gjeneron kampanja automatike (10% probabilitet/ditë)
- 3 tipe: Discount (50%), Bundle (30%), Loyalty (20%)
- Zbritje: 5-30% me ndikim direkt në kërkesë (demand lift 10-35%)
- ROI kalkulim automatik

---

## 📐 Bazat Matematikore

### Procesi Poisson jo-homogjen (NHPP)
Kërkesa e klientëve modelohet si NHPP sepse λ ndryshon me kohën:
```
N(t) ~ Poisson(λ(t))
λ(t) = f(orë, ditë, muaj, ngjarje)
```

### Shpërndarja Negative Binomiale për Basket Size
Madhësia e shportës përdor parametra (μ, σ) për çdo tip klienti — kjo lejon "tail" të gjatë (blerjet e mëdha janë të rralla por ekzistojnë).

### EOQ (Economic Order Quantity)
```
EOQ = √(2DS/H)
D = kërkesa vjetore
S = kosto porosie
H = kosto mbajtjeje/njësi
```

### ABC/XYZ Analysis
**ABC** klasifikon produktet sipas vlerës (Pareto 80/20):
- A: 20% produkte → 80% revenue
- B: 30% produkte → 15% revenue  
- C: 50% produkte → 5% revenue

**XYZ** klasifikon sipas variabilitetit të kërkesës:
- X: Kërkesë stabile (produkte bazë)
- Y: Kërkesë sezonale (fruta/perime)
- Z: Kërkesë sporadike (produkte luksi)

### Monte Carlo — Risk Assessment
Simulimi i Black Swan Events përdor parametra stokastikë:
```python
event_mult = max(0.3, 1.0 - (intensity/10) × 0.7)  # Luftë
```

---

## 📁 Struktura e Projektit

```
supply-chain-management/
│
├── 📁 database/
│   └── schema.sql                    # 22 tabela PostgreSQL + RLS + Indexes
│
├── 📁 world_config/
│   ├── generate_products.py          # Gjeneron 500 produkte → Supabase
│   ├── generate_stores.py            # 15 supermarkete me koordinata
│   ├── generate_routes.py            # Matricë distancash (OpenRouteService)
│   └── seed_all.py                   # Setup i plotë 1 herë
│
├── 📁 simulation/
│   ├── demand_profile.py             # NHPP + sezonalitet + Black Swan
│   ├── sales_module.py               # Transaksione + Basket Distribution
│   ├── inventory_module.py           # Stoku + Expired + Restock
│   ├── transport_module.py           # Dërgesa + Kosto karburantit
│   ├── warehouse_module.py           # Snapshots magazinave
│   ├── purchasing_module.py          # Purchase Orders + Lead Time
│   └── marketing_module.py          # Kampanja + ROI
│
├── 📁 aggregation/
│   ├── daily_aggregator.py           # Agregim çdo natë → sales_daily
│   └── monthly_aggregator.py        # Agregim fund muaji → kpi_monthly
│
├── 📁 api/
│   ├── main.py                       # FastAPI Gateway + CORS + Error Handler
│   ├── auth.py                       # API Key Authentication
│   ├── 📁 routers/
│   │   ├── sales.py                  # API 1: Transaksione, KPI shitjesh
│   │   ├── inventory.py              # API 2: Stoku, Alerts, Log
│   │   ├── logistics.py              # API 3: Transport, Magazina
│   │   └── procurement.py           # API 4: Porosi, Furnizues, Marketing
│   └── 📁 schemas/
│       ├── sales_schema.py           # Pydantic models
│       ├── inventory_schema.py
│       ├── logistics_schema.py
│       └── procurement_schema.py
│
├── 📁 scheduler/
│   └── scheduler.py                  # APScheduler 06:00-22:00 + Manual Run
│
├── 📁 config/
│   ├── settings.py                   # Supabase client + Environment vars
│   └── constants.py                  # Parametrat globalë statistikorë
│
├── 📁 streamlit/
│   └── app.py                        # Admin Control Panel + Event Injector
│
├── .env                              # 🔒 Kredencialet (nuk shkon në Git)
├── .env.example                      # Template publik
├── .gitignore
├── requirements.txt
├── render.yaml                       # Deploy config për Render
└── README.md
```

---

## 🗄️ Databaza

### Struktura (22 Tabela)

**Tabela Reference (8)** — krijohen 1 herë, statike:
| Tabela | Rreshta | Përshkrim |
|--------|---------|-----------|
| `product_categories` | 8 | Kategoritë e produkteve |
| `suppliers` | 10 | Furnizuesit shqiptarë |
| `products` | 500 | Produktet me ABC/XYZ klasë |
| `stores` | 15 | Supermarketet + λ_final |
| `warehouses` | 5 | 1 qendrore + 4 rajonale |
| `vehicles` | 15 | Kamionët standard/frigorifer |
| `drivers` | 15 | Shoferët |
| `routes` | 15 | Rrugët store→magazinë |

**Tabela Operative (7)** — mbushen automatikisht nga simulimi:
| Tabela | Rreshta/ditë | Përshkrim |
|--------|-------------|-----------|
| `transactions` | ~14,400 | Faturat e klientëve |
| `inventory_log` | ~9,600 | Lëvizjet e stokut |
| `shipments` | ~30 | Dërgesat ditore |
| `purchase_orders` | ~10 | Porosi te furnizuesit |
| `warehouse_snapshot` | ~80 | Gjendja e magazinave |
| `campaigns` | ~0-1 | Kampanjat marketing |
| `fuel_prices` | 1 | Çmimi karburantit |

**Tabela Agregat (6)** — ruajnë historikun efikas:
| Tabela | Rreshtat/ditë | Ruhet |
|--------|--------------|-------|
| `sales_hourly` | ~240 | Gjithmonë |
| `sales_daily` | ~15 | Gjithmonë |
| `sales_monthly` | ~15 | Gjithmonë |
| `inventory_daily` | ~15 | Gjithmonë |
| `transport_daily` | ~15 | Gjithmonë |
| `kpi_monthly` | ~15 | Gjithmonë |

**Tabela Speciale:**
- `simulation_config` — 18 parametra të kontrollueshëm në kohë reale

---

## 🔌 API Endpoints

Baza URL: `https://supply-chain-api.onrender.com`

**Authentication:** Çdo endpoint kërkon `X-API-Key` header.

### API 1 — Sales (`/api/v1/sales/`)
| Method | Endpoint | Përshkrim |
|--------|----------|-----------|
| GET | `/transactions` | Faturat e fundit (filter: store, product) |
| GET | `/transactions/store/{id}` | Faturat për 1 store |
| GET | `/summary/hourly` | Agregim orësh |
| GET | `/summary/daily` | Agregim ditor |
| GET | `/summary/monthly` | Agregim mujor |
| GET | `/kpi` | KPI kryesore shitjesh |

### API 2 — Inventory (`/api/v1/inventory/`)
| Method | Endpoint | Përshkrim |
|--------|----------|-----------|
| GET | `/stock/{store_id}` | Stoku aktual i store-it |
| GET | `/log` | Log lëvizjeve (Sale/Restock/Expired) |
| GET | `/alerts` | Produkte nën reorder point |
| GET | `/daily` | Agregim ditor stoku |

### API 3 — Logistics (`/api/v1/logistics/`)
| Method | Endpoint | Përshkrim |
|--------|----------|-----------|
| GET | `/shipments` | Të gjitha dërgesat |
| GET | `/shipments/active` | Dërgesa "In Transit" |
| GET | `/warehouse/snapshot` | Gjendja e magazinave |
| GET | `/routes/performance` | KPI rrugësh |
| GET | `/kpi` | OTD%, kosto, vonesë |

### API 4 — Procurement (`/api/v1/procurement/`)
| Method | Endpoint | Përshkrim |
|--------|----------|-----------|
| GET | `/orders` | Purchase Orders |
| GET | `/orders/pending` | Porosi aktive |
| GET | `/suppliers/scorecard` | KPI furnizuesish |
| GET | `/campaigns` | Kampanjat marketing |
| GET | `/campaigns/roi` | ROI kampanjave |

**Swagger UI:** `https://supply-chain-api.onrender.com/docs`

---

## 🛠️ Teknologjitë e Përdorura

| Teknologji | Versioni | Pse u zgjodh |
|-----------|---------|--------------|
| **Python** | 3.11+ | Standard industrie për Data Engineering |
| **FastAPI** | 0.135 | Performancë e lartë, async, Swagger automatik |
| **Supabase** | 2.28 | PostgreSQL managed, RLS, API falas |
| **PostgreSQL** | 15 | Databazë relacionale e fuqishme, SQL standard |
| **NumPy** | 2.3 | Llogaritje statistikore, shpërndarje Poisson |
| **APScheduler** | 3.11 | Scheduling i saktë me Cron triggers |
| **Pydantic** | 2.12 | Validim i dhënash, schema definition |
| **httpx** | 0.28 | HTTP client async për Supabase |
| **python-dotenv** | 1.2 | Menaxhim i sigurt i kredencialeve |
| **Render** | — | Deploy falas, CI/CD automatik |
| **GitHub** | — | Version control, colaborim |
| **Power BI** | — | Dashboard enterprise, Web Connector |

---

## 🎓 Njohuritë Akademike

### Kërkime Operacionale
- **EOQ Model** — Sasia optimale e porosisë: `√(2DS/H)`
- **Reorder Point** — Trigger automatik: `ROP = d × L + z × σ_L`
- **Route Optimization** — Minimizim kostosh transporti
- **Network Flow** — Furnizim magazinë → store

### Optimizim
- **Linear Programming** — Minimizim kosto zinxhiri furnizimi
- **Safety Stock** — `SS = z × σ_d × √L`
- **Shelf Space Allocation** — ABC klasifikimi
- **Multi-Criteria Supplier Selection** — Kosto vs Lead Time vs Cilësi

### Probabilitet dhe Proceset e Rastit
- **Poisson jo-homogjen (NHPP)** — Arrdhja e klientëve: `λ(t)` ndryshon me kohën
- **Negative Binomial** — Shpërndarja e madhësisë së shportës
- **Zinxhirët e Markov** — Gjendje stoku: i plotë → i ulët → stockout
- **Monte Carlo** — Simulim riskut nga Black Swan Events
- **GBM (Geometric Brownian Motion)** — Modelim çmimesh dinamike
- **SVD (Singular Value Decomposition)** — Analiza e matricës së kërkesës (analizë, jo kompresim)

### Supply Chain Analytics (4 Shtresat)
1. **Descriptive** — Çfarë ndodhi? (Dashboard Power BI)
2. **Diagnostic** — Pse ndodhi? (Root cause analysis)
3. **Predictive** — Çfarë do të ndodhë? (ARIMA, Forecasting)
4. **Prescriptive** — Çfarë duhet bërë? (Optimizim rrugësh, reorder)

---

## 🚀 Instalimi

### Kërkesat
```bash
Python 3.11+
Git
Llogari Supabase (falas)
Llogari Render (falas)
```

### Klono dhe Instalo
```bash
# Klono repositorin
git clone https://github.com/ermirhaxhia/supply-chain-management.git
cd supply-chain-management

# Instalo dependencies (globalisht ose në venv)
pip install -r requirements.txt
```

### Konfiguro Databazën
```bash
# 1. Krijo projekt të ri në supabase.com
# 2. Shko te SQL Editor
# 3. Kopjo dhe ekzekuto përmbajtjen e database/schema.sql
```

---

## ⚙️ Konfigurimi

Krijo file `.env` në root të projektit:

```env
# Supabase
SUPABASE_URL=https://xxxxxxxxxxxx.supabase.co
SUPABASE_SERVICE_KEY=eyJ...

# API Keys (gjenero vetë)
API_KEY_MASTER=sc_master_xxxxxxxxxxxxxxxxx
API_KEY_SALES=sc_sales_xxxxxxxxxxxxxxxx
API_KEY_INVENTORY=sc_inv_xxxxxxxxxxxxxxxx
API_KEY_LOGISTICS=sc_log_xxxxxxxxxxxxxxxx
API_KEY_PROCUREMENT=sc_proc_xxxxxxxxxxxxxxx

# Simulim
FUEL_BASE_PRICE=185
```

**Gjenero API Keys:**
```bash
python -c "import secrets; [print(f'KEY_{i}: sc_{secrets.token_hex(12)}') for i in range(5)]"
```

---

## 📖 Përdorimi

### Setup Fillestar (1 herë)
```bash
# Gjenero 500 produktet
python world_config/generate_products.py

# Verifiko databazën
python -c "from config.settings import supabase; print('OK')"
```

### Run Manual (Test)
```bash
# Ekzekuto 1 tick simulimi (mbush databazën)
python scheduler/scheduler.py --manual
```

### Starto API-n
```bash
# Terminal 1 — API Server
python api/main.py
# API aktive në: http://localhost:8000
# Swagger UI:   http://localhost:8000/docs
```

### Starto Scheduler-in
```bash
# Terminal 2 — Scheduler (06:00-22:00)
python scheduler/scheduler.py
# Ctrl+C për të ndaluar
```

### Testo API-n
```bash
# Me curl
curl -H "X-API-Key: sc_master_xxx" http://localhost:8000/api/v1/sales/transactions

# Ose hap Swagger UI dhe fut API Key
http://localhost:8000/docs → Authorize → sc_master_xxx
```

---

## 📈 Projeksionet e të Dhënave

### Revenue Ditor (të gjitha 15 store-et)
```
Ora  6:  ~120,000  Lekë   (hapje, i qetë)
Ora 12:  ~2,800,000 Lekë  (PEAK drekë)
Ora 18:  ~3,100,000 Lekë  (SUPER PEAK)
Ora 22:  ~200,000  Lekë   (mbyllje)

TOTAL/ditë:  ~25-30M Lekë
TOTAL/muaj:  ~750M - 900M Lekë (≈€6.8M-8.2M)
```

### Madhësia e Databazës
```
Raw data (30 ditë):    ~249 MB
Agregat historik:      ~1 MB/muaj
Jetëgjatësia totale:   ~4+ vjet (brenda 500MB Supabase free)
```

---

## 🦢 Black Swan Events

Sistemi suporton injektim dinamik të ngjarjeve të paparashikuara përmes `simulation_config`:

| Event | `active_event` | Ndikimi |
|-------|---------------|---------|
| Normal | 0 | Asgjë |
| Luftë/Krizë | 1 | Kërkesë -70%, Karburant +35%, Lead time ×2.5 |
| Pandemi | 2 | Kërkesë -80%, Supply chain disruption |
| Grevë Transporti | 3 | Transport -40%, Vonesa +60 min |
| Thatësirë | 4 | Çmimet ushqimeve +30% |

**Ndryshim në kohë reale:**
```sql
UPDATE simulation_config 
SET config_value = 1.0  -- Aktivizo Luftë
WHERE config_key = 'active_event';

UPDATE simulation_config 
SET config_value = 7.0  -- Intensitet 7/10
WHERE config_key = 'event_intensity';
```

---

## 💾 Strategjia e Ruajtjes së Të Dhënave

Sistemi përdor **Agregim të Shtresëzuar** (3-tier archiving):

```
SHTRESA 1 — RAW (30 ditët e fundit)
├── transactions    ~14,400 rreshta/ditë
├── inventory_log   ~9,600 rreshta/ditë
└── Fshihet automatikisht pas 30 ditësh

SHTRESA 2 — AGREGIM ORËSH (gjithmonë)
├── sales_hourly    ~240 rreshta/ditë
└── Ruan serinë kohore për ARIMA

SHTRESA 3 — AGREGIM DITOR/MUJOR (gjithmonë)
├── sales_daily, inventory_daily    ~15 rreshta/ditë
├── sales_monthly, kpi_monthly      ~15 rreshta/muaj
└── KPI financiare historike

REZULTAT: 500MB Supabase Free → ~4+ vjet historik
```

**Pse kjo qasje:**
- Raw data → analiza granulare (basket analysis, SVD)
- Agregim orësh → Time Series (ARIMA, STL Decomposition)
- Agregim mujor → KPI financiare (Power BI reports)
- **Asnjë gabim absolut** — operacionet janë SUM/AVG/COUNT (matematikisht ekzakte)

---



### Fusha për Zgjerim
- Implementim ARIMA mbi të dhënat historike
- Dashboard Streamlit i avancuar
- Integrimi me OpenRouteService API

---

## 👤 Autori

**Ermir Haxhia**  
MSc Applied Mathematics — Universiteti i Tiranës  

🌐 [Portfolio]([https://ermir-haxhia.vercel.app](https://infrequent-network-348.notion.site/Ermir-Haxhia-Data-Analysis-Portfolio-32c61957cde1800ebff6d7c1190170ae)) 
· [LinkedIn](https://www.linkedin.com/in/ermir-haxhia-b988212b5) 
· [GitHub](https://github.com/ermirhaxhia)

---

> *"Ky projekt nuk simulon vetëm një zinxhir furnizimi — ai e ndërton atë nga zeroja, duke aplikuar matematikën në probleme reale të industrisë."*
