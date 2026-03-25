# Auditim teknik — Supply Chain Management (Python)

**Data e auditimit:** 2026-03-24  
**Fushëllëkura:** modulët në `config/`, `simulation/`, `world_config/`, `database/schema.sql`, `requirements.txt`

---

## 1. Analiza e kodit (Pythonic way)

### 1.1 PEP 8 (stili)

| Vlerësim | Shënim |
|----------|--------|
| Pjesërisht në përputhje | Kodi është i lexueshëm dhe i strukturuar me komente, por ka disa devijime të zakonshme nga PEP 8. |

**Çfarë u vërejt:**

- **Ritranshekime të gjata:** `world_config/generate_products.py` përmban shumë rreshta që kalojnë kufirin e zakonshëm 79/88 karakterësh të PEP 8.
- **Hapësira në anotime:** përdoren zgjërime të jashtëzakonshme për të rreshtuar parametra (psh. `store_id:    str`), që PEP 8 i trajton si të pranueshme por jo si idiomën më të zakonshme.
- **`sys.path.append(...)`** në çdo modul simulimi — funksionon, por nuk është modeli i rekomanduar (më mirë: paketë e instalueshme ose `PYTHONPATH` / layout me `src/`).
- **`logging.basicConfig`** i përsëritur në shumë skedarë — i vetmi “fitues” është thërretja e parë; të tjerët zakonisht injorohen, që bën sjelljen e logging-ut të paparashikueshme sipas rendit të importit.

**Rekomandim:** të ekzekutohet një mjet si `ruff check .` ose `flake8` + (opsionale) `black`, dhe të centralizohet konfigurimi i logging-ut një herë (psh. në `config/settings.py` ose në hyrjen e aplikacionit).

---

### 1.2 Argumente default të mutueshme (`def f(a=[])`)

**Gjetje:** Nuk u gjet përdorim i `=[]` ose `={}` si argument default në nënshkrimet e funksioneve. **Në rregull.**

Shënim: listat/diktet në nivel moduli (psh. `_products_cache: list = []`) **nuk** janë e njëjta gjë si default-et mutueshëm të argumenteve; ato janë idiomë e pranueshme për cache, me kujdes për thread-safety nëse një ditë kaloni në ekzekutim paralel.

---

### 1.3 `try` / `except` shumë të përgjithshëm

**Bare `except:`** (pa tip) **nuk u gjet** — mirë.

Por **`except Exception as e`** përdoret shpesh në të gjithë projektin. Kjo **nuk** është e njëjta gjë si `except:`, por prapë:

- fsheh ndërprerje të qëllimshme nëse nuk e ribën `raise` pas logimit;
- e bën diagnostikimin më të vështirë krahasuar me `except (SupabaseError, KeyError, …)`.

Shembull tipik (shumë të ngjashëm në module të tjera):

```python
try:
    response = supabase.table("products").select("*").execute()
    # ...
except Exception as e:
    logger.error(f"❌ ERROR duke ngarkuar produktet: {e}")
```

**Rekomandim:** të kapen përjashtime specifike të librarisë (`httpx`, `postgrest`, etj.) ku dokumentacioni i jep, dhe për logjikë biznesi `KeyError` / `ValueError`; për gabime të papritura, `raise` pas logimit ose `except Exception` vetëm në kufij të qartë (CLI, worker).

---

## 2. Menaxhimi i varësive (dependencies)

### 2.1 `requirements.txt` vs `pyproject.toml`

- Ekziston vetëm **`requirements.txt`** (nuk ka `pyproject.toml` në repo).
- Versionet janë të fiksuara me `==`, që është **e mirë** për riprodhueshmëri.

### 2.2 Përputhja me kodin aktual

Në kod **nuk** shfaqet import i **`fastapi`**, **`uvicorn`**, **`apscheduler`**, o **`httpx`** (kërkim në të gjithë `.py`). Këto mund të jenë për një shërbim të planifikuar ose të lënë “mbingarkesë”; për projektin aktual të simulimit/Supabase, janë **varësi të papërdorura ose jo të verifikuara në këtë repo**.

### 2.3 Varësi dev vs prod

**Nuk** ka ndarje formale:

- nuk ka `requirements-dev.txt`, `pyproject.toml [project.optional-dependencies]`, as grupe `pip install '.[dev]'`.

**Rekomandim:** të ndahen `pytest`, `ruff`, `mypy` (ose të ngjashme) në një skedar dev, ose të migrohet në `pyproject.toml` me extra `dev`.

### 2.4 Siguria dhe “outdated”

Nuk u arrit të ekzekutohet me sukses `pip-audit` përmes `python -m pip audit` në këtë mjedis (komanda nuk ishte e disponueshme si e tillë). Për audit të vazhdueshëm të CVE-ve rekomandohet lokalishisht:

```bash
pip install pip-audit
pip-audit -r requirements.txt
```

Gjithashtu: të kontrollohet që versionet e `numpy` / `pandas` të jenë të përputhshme me versionin e Python që përdorni (p.sh. Python 3.13 kërkon wheel të përditësuara).

---

## 3. Performanca dhe memoria

### 3.1 Lista / kërkim / përsëritje

**`select_products_for_basket` (family):** për çdo kategori zgjedhjeje, bëhet një list comprehension mbi të gjithë `pool`:

```python
for cat in np.random.choice(categories, min(len(categories), basket_size), replace=False):
    cat_products = [p for p in pool if p["category_id"] == cat]
```

Kompleksiteti mund të afrohet **O(num_categories × N)** për këtë degë. Për shumë produkte, mund të optimizohet duke ndërtuar një herë një `dict[category_id, list[product]]` ose `defaultdict(list)`.

**`list(CUSTOMER_TYPES.keys())` dhe `list(CUSTOMER_TYPES.values())`** në çdo transaksion — mund të ngelen si tuple/lista konstante në nivel moduli për të shmangur alokime të përsëritura në `run_sales_hour`.

### 3.2 Kompleksitet në funksionet kryesore (përmbledhje)

| Funksioni / rrjedha | Kompleksitet i përafërt | Shënim |
|---------------------|-------------------------|--------|
| `initialize_stock` | O(S × P) | S = dyqane, P = produkte — i pashmangshëm për matricën e plotë në memorie. |
| `run_sales_hour` | O(C × cost_txn) | C ~ klientë nga Poisson; `generate_transaction` është O(madhësia e shportës) (~ konstant i vogël). |
| `run_purchasing` | O(P) për dyqan/ditë | Iteron të gjithë produktet; në rregull për skallëzim të moderuar. |
| `get_lambda` / `get_customers` | O(1) për çdo thirrje | Lookup në dict për trend/orë/festë. |
| `load_simulation_config` | O(rreshta) | Një herë, pastaj cache. |

### 3.3 Sjellje që ndikon në logjikë / matje

Në `run_sales_hour`, pas një transaksioni të suksesshëm, **`get_basket_type(dt)` thirret përsëri** — kjo gjeneron **një tip të ri të shportës**, jo atë që përdori `generate_transaction` brenda. Statistikat `basket_type_stats` **nuk pasqyrojnë** shportën reale të faturës.

```python
for _ in range(num_customers):
    txn = generate_transaction(store, products, dt)
    if txn:
        transactions.append(txn)
        total_revenue += txn["total"]
        basket_type = get_basket_type(dt)  # ← tip i ri, i pavarur nga txn
        basket_type_stats[basket_type] = basket_type_stats.get(basket_type, 0) + 1
```

**Rregullim i sugjeruar:** të kthehet `basket_type` nga `generate_transaction` (ose të vendoset në `txn`) dhe të përdoret ai për statistika.

---

## 4. Siguria

### 4.1 SQL dhe f-strings

Në Python **nuk** përdoret ndërtim i query-ve SQL me f-string për ekzekutim drejtpërdrejt. Qasja është përmes klientit **Supabase** (`table(...).select(...).insert(...)`), ku emrat e tabelave janë të fiksuar në kod, jo hyrje përdoruesi. **Rreziku klasik i SQL injection nga f-strings në SQL të papërgatitur është i ulët në këtë shtresë.**

Schema në `database/schema.sql` është SQL statik — jo e ekspozuar si string e montuar nga përdoruesi në Python.

### 4.2 Të dhëna sensitive (API keys, etj.)

- **`config/settings.py`** lexon `SUPABASE_URL`, `SUPABASE_SERVICE_KEY` dhe çelësat API nga **mjedisi** (`os.getenv`) — praktikë e mirë.
- **`.gitignore`** përfshin `.env` — redukton rrezikun e commit të fshehtëzave.

Nuk u gjet në kod hardcodim i dukshëm i çelësave (kërkim për emra tipikë). **Vazhdoni të mos vendosni `.env` në Git.**

---

## 5. Testimi dhe mbulimi

### 5.1 Gjendja aktuale

- **Nuk u gjet asnjë skedar** `test_*.py` ose dosje `tests/` me **pytest**.
- Mbulimi me teste automatikë është praktikisht **0%** (vetëm blloqe `if __name__ == "__main__"` manuale).

### 5.2 Ku të shtohen teste me `pytest` (prioritet)

1. **`simulation/demand_profile.safe_get_lambda`**: `get_lambda` — me `store` dhe `datetime` të fiksuar, të asertohen multipliers (trend, weekly, hourly) pa thirrje rrjeti (mock `get_config` nëse duhet).
2. **`simulation/sales_module`**: `get_basket_size` kufij min/max sipas `BASKET_SIZE_PARAMS`; `get_basket_type` të kthejë vlera të lejuara.
3. **`select_products_for_basket`**: me listë të vogël produktesh të mock-uar (dict me `category_id`, `abc_class`) — të kontrollohet që për `basket_type="quick"` zgjedhja vjen nga pool-i i pritur.
4. **`simulation/purchasing_module`**: `has_active_order` / `register_order` / `complete_order` — state i `_active_orders`; `generate_purchase_order` kur stoku është ulët vs kur porosia ekziston.
5. **`run_purchasing` — edge case:** nëse gjatë `try` ngjit një përjashtim **para** `inserted = 0`, variabla `inserted` **mund të mos ekzistojë** dhe rreshti i `stats` / `logger.info` mund të sjellë `NameError`. Test që simulon përjashtim në `for product in products` dhe pret sjellje të kontrolluar.

Për integrim me Supabase: përdorni **fixtures** me klient të mock-uar ose mjedise test (projekt i veçantë Supabase), jo prod.

**Strukturë e sugjeruar:**

```text
tests/
  conftest.py          # fixtures, mock supabase
  test_demand_profile.py
  test_sales_basket.py
  test_purchasing.py
```

---

## 6. Probleme shtesë që vlejnë korrigjim

### 6.1 `run_purchasing` dhe `inserted` jashtë `try`

```python
    try:
        store_stock = stock_cache.get(store_id, {})
        for product in products:
            # ... mund të hedhë exception para inserted = 0
            ...
        inserted = 0
        if orders:
            ...
    except Exception as e:
        logger.error(f"❌ ERROR në run_purchasing: {e}")

    stats = {
        "orders_created": inserted,  # ← NameError nëse exception i hershëm
        ...
    }
```

**Rregullim i minimizuar:** `inserted = 0` para `try`, ose `except` që vendos `inserted = 0`.

---

## 7. Përmbledhje ekzekutive

| Fushë | Nota e shkurtër |
|--------|------------------|
| PEP 8 | E mirë strukturalisht; përmirëso me linter/formatter dhe logging të unifikuar. |
| Default mutueshëm | Nuk ka problem të identifikuar. |
| `except` | Pa `bare except`; shumë `Exception` — specifikoni ku mundeni. |
| Varësi | Fiksime të mira; mungon ndarja dev/prod; disa paketa nuk duken të përdora në kod. |
| Performancë | Njëhershitje të mundshme në zgjedhjen e produkteve për shportë “family”; bug në statistikat e basket type. |
| SQL injection (f-string) | I ulët në shtresën aktuale Supabase. |
| Secrets | Mjedis + `.gitignore` — në rregull. |
| Teste | Mungojnë — shtoni `pytest` dhe filloni nga funksionet pure (demand, basket, purchasing state). |

Ky dokument mund të përditet si checklist për një sprint “hardening” (teste + linter + rregullime të vogla të sigurta llogjike).
