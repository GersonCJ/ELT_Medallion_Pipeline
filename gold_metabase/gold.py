"""
=============================================================
ELT Medallion Pipeline — Pipeline Gold
=============================================================
Useful to:
  1. Create business tables (gold layer) using data from dbt marts
  2. Automate dashboard Metabase creation
  
  Tabelas geradas:
  1. Dashboard_Combinacoes_Ouro: Recomendações de ingredientes
  2. Dashboard_Saturation_ROI: Análise custo-benefício
  3. Dashboard_Controverso_Por_Pele: Impacto de alergênicos
  4. Dashboard_Premium_Whitespace: Oportunidades de mercado
  5. Dashboard_Benchmark_Competitivo: Análise comparativa
  6. Dashboard_Recomendacoes_Inovacao: Sugestões de produtos

Execution:
  python gold_metabase/gold.py

Environment variables (via .env):
  DB_HOST, DB_PORT, DB_USER,
  DB_PASSWORD, DB_NAME, METABASE_HOST,
  METABASE_USER, METABASE_PASSWORD
=============================================================
"""

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import logging
import os
import sys
import requests
import time


# ─────────────────────────────────────────────
# 1. Logging Config
# ─────────────────────────────────────────────
import os
from pathlib import Path

# Detect execution environments
is_docker = os.path.exists("/.dockerenv")
if is_docker:
    log_dir = "/app/gold_metabase"
    log_file = "/app/gold_metabase/gold.log"
else:
    log_dir = "gold_metabase"
    log_file = "gold_metabase/gold.log"

# Create logs dir if it doesn't exist
Path(log_dir).mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 2. Load env variables
# ─────────────────────────────────────────────
load_dotenv()

POSTGRES_HOST = os.getenv("DB_HOST")
POSTGRES_PORT = os.getenv("DB_PORT")
POSTGRES_USER = os.getenv("DB_USER")
POSTGRES_PASSWORD = os.getenv("DB_PASSWORD")
POSTGRES_DB = os.getenv("DB_NAME")

DB_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ─────────────────────────────────────────────
# 2b. Metabase config
# ─────────────────────────────────────────────
METABASE_HOST = os.getenv("METABASE_HOST")
METABASE_USER = os.getenv("METABASE_USER")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD")
METABASE_SCHEMA = "gold"


# ─────────────────────────────────────────────
# MetabaseAPI Class - Metabase Integration
# ─────────────────────────────────────────────

class MetabaseAPI:
    """Performs Metabase initial setup via API if not already configured."""
    
    def __init__(self, host, username, password):
        self.host = host
        self.base_url = f"{host}/api"
        self.auth = {"username": username, "password": password}
        self.session_id = None
        self.logger = logger
        self.logger.info(f"Metabase host raw: {host}")
        self.base_url = f"{host}/api"
        self.logger.info(f"Metabase base_url: {self.base_url}")

    def first_time_setup(self):
        """Setup user for the first time."""
        try:
            # Verifica se já foi configurado
            r = requests.get(f"{self.base_url}/session/properties", timeout=5)
            props = r.json()
            
            if props.get("has-user-setup") is True:
                self.logger.info("Metabase already configured, skipping setup")
                return True
            
            self.logger.info("Metabase setup wizard — initializing automatic configuration...")
            
            # Obtém o setup token
            setup_token = props.get("setup-token")
            if not setup_token:
                self.logger.error("Setup-token not found")
                return False
            
            # Monta o payload de setup
            payload = {
                "token": setup_token,
                "user": {
                    "email": self.auth["username"],
                    "password": self.auth["password"],
                    "first_name": "Admin",
                    "last_name": "ELT",
                    "site_name": "ELT Pipeline"
                },
                "prefs": {
                    "site_name": "ELT Pipeline",
                    "allow_tracking": False
                }
            }
            
            r = requests.post(
                f"{self.base_url}/setup",
                json=payload,
                timeout=10
            )
            
            if r.status_code == 200:
                self.logger.info("Metabase setup concluded automatically")
                return True
            else:
                self.logger.error(f"Setup error: {r.status_code} - {r.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Automatic setup error: {e}")
            return False

    def authenticate(self):
        """Autenticate with Metabase server."""
        self.logger.info(f"Calling Metabase: {self.base_url}/session")
        try:
            resp = requests.post(
                f"{self.base_url}/session",
                json=self.auth,
                timeout=10
            )
            if resp.status_code == 200:
                self.session_id = resp.json()["id"]
                self.logger.info("Metabase: Autenticated successfully")
                return True
        except Exception as e:
            self.logger.error(f"Error autenticating on Metabase: {e}")
        return False
    
    def add_database(self, db_name, host, port, user, password):
        """Connect PostgreSQL database to Metabase."""
        try:
            payload = {
                "name": db_name,
                "engine": "postgres",
                "details": {
                    "host": host,
                    "port": int(port),
                    "dbname": db_name,
                    "user": user,
                    "password": password,
                    "ssl": False,
                    "tunnel-enabled": False
                }
            }
            resp = requests.post(
                f"{self.base_url}/database",
                headers=self.get_headers(),
                json=payload,
                timeout=15
            )
            if resp.status_code == 200:
                db_data = resp.json()
                self.logger.info(f"Database '{db_name}' connected to Metabase (ID: {db_data['id']})")
                return db_data
            else:
                self.logger.error(f"Error connecting database: {resp.status_code} - {resp.text}")
        except Exception as e:
            self.logger.error(f"Error connecting database: {e}")
        return None
    
    def sync_database(self, db_id):
        """Trigger metadata sync and wait for tables to be available."""
        try:
            # Trigger sync
            resp = requests.post(
                f"{self.base_url}/database/{db_id}/sync_schema",
                headers=self.get_headers(),
                timeout=10
            )
            if resp.status_code != 200:
                self.logger.warning(f"Sync trigger returned: {resp.status_code}")

            self.logger.info("Waiting for Metabase to sync database schema...")
            time.sleep(15)  # Give Metabase time to scan all tables
            return True
        except Exception as e:
            self.logger.error(f"Error triggering sync: {e}")
            return False

    def get_headers(self):
        """Return headers to autenticated requests."""
        return {
            "X-Metabase-Session": self.session_id,
            "Content-Type": "application/json"
        }

    def get_db_id(self, db_name):
        """Obtain database ID by name."""
        try:
            resp = requests.get(
                f"{self.base_url}/database",
                headers=self.get_headers(),
                timeout=10
            )
            databases = resp.json()
            
            # Handle both response formats
            if isinstance(databases, dict):
                databases = databases.get("data", [])
            
            for db in databases:
                if db["name"] == db_name:
                    return db["id"]
                    
            self.logger.warning(f"Database '{db_name}' not found. Available: {[d['name'] for d in databases]}")
        except Exception as e:
            self.logger.error(f"Error obtaining DB ID: {e}")
        return None

    def get_table_id(self, db_id, schema, table_name):
        """Obtain tabel ID by schema and tabel's name."""
        try:
            resp = requests.get(
                f"{self.base_url}/database/{db_id}/metadata",
                headers=self.get_headers(),
                timeout=10
            )
            for table in resp.json().get("tables", []):
                if table["schema"] == schema and table["name"] == table_name:
                    return table["id"]
        except Exception as e:
            self.logger.error(f"Error in obtaining Table ID: {e}")
        return None

    def create_card(self, db_id , table_id, config):
        """create a card (graphic) in Metabase."""
        try:
            payload = {
                "name": config["title"],
                "description": config.get("description", ""),
                "display": config["display_type"],
                "visualization_settings": config.get("visualization_settings", {}),
                "dataset_query": {
                    "database": db_id,
                    "type": "query",
                    "query": {"source-table": table_id}
                }
            }

            resp = requests.post(
                f"{self.base_url}/card",
                headers=self.get_headers(),
                json=payload,
                timeout=10
            )
            if resp.status_code == 200:
                card_data = resp.json()
                self.logger.info(f"Card created: {config['title']} (ID: {card_data['id']})")
                return card_data
            else:
                self.logger.error(f"Error to create card {config['title']}: {resp.text}")
        except Exception as e:
            self.logger.error(f"Error to create card: {e}")
        return None

    def create_dashboard(self, name, description=""):
        """Create a new dashboard on Metabase."""
        try:
            payload = {
                "name": name,
                "description": description
            }
            resp = requests.post(
                f"{self.base_url}/dashboard",
                headers=self.get_headers(),
                json=payload,
                timeout=10
            )
            if resp.status_code == 200:
                dash_data = resp.json()
                self.logger.info(f"Dashboard creates: {name} (ID: {dash_data['id']})")
                return dash_data
            else:
                self.logger.error(f"Error on creating the dashboard: {resp.text}")
        except Exception as e:
            self.logger.error(f"Error to create dashboard: {e}")
        return None

    def add_card_to_dashboard(self, dash_id, card_id, row, col=0, size_x=20, size_y=15):
        """Add a card to dashboard."""
        try:
            # New Metabase API: GET current cards first, then PUT the full updated list
            resp = requests.get(
                f"{self.base_url}/dashboard/{dash_id}",
                headers=self.get_headers(),
                timeout=10
            )
            current_cards = resp.json().get("dashcards", [])

            current_cards.append({
                "id": -1,  # Temporary ID for new card
                "card_id": card_id,
                "row": row,
                "col": col,
                "size_x": size_x,
                "size_y": size_y,
                "visualization_settings": {}
            })

            payload = {"cards": current_cards}

            resp = requests.put(
                f"{self.base_url}/dashboard/{dash_id}/cards",
                headers=self.get_headers(),
                json=payload,
                timeout=10
            )
            if resp.status_code == 200:
                self.logger.info(f"Card {card_id} added to dashboard {dash_id}")
                return True
            else:
                self.logger.error(f"Error adding card to dashboard: {resp.status_code} - {resp.text}")
        except Exception as e:
            self.logger.error(f"Error adding card to dashboard: {e}")
        return False

    def publish_dashboard(self, dash_id):
        """Publish dashboard."""
        try:
            payload = {"canned_embedding_params": {}}
            resp = requests.put(
                f"{self.base_url}/dashboard/{dash_id}",
                headers=self.get_headers(),
                json=payload,
                timeout=10
            )
            if resp.status_code == 200:
                self.logger.info(f"Dashboard {dash_id} publicado")
                return True
        except Exception as e:
            self.logger.error(f"Erro ao publicar dashboard: {e}")
        return False


# ─────────────────────────────────────────────
# 3. Support functions
# ─────────────────────────────────────────────

def get_engine():
    """Create connexion with PostgreSQL DB with retry."""
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Attempt {attempt}/{max_retries} of database connexion...")
            engine = create_engine(DB_URL, echo=False, connect_args={'connect_timeout': 10})
            
            # Testa a conexão
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                conn.commit()
            
            logger.info("Database connexion established")
            return engine
            
        except Exception as e:
            logger.warning(f"Error attempt {attempt}: {e}")
            
            if attempt < max_retries:
                logger.info(f"Awaiting {retry_delay}s before next attempt...")
                time.sleep(retry_delay)
            else:
                logger.error("Database connexion failed after every attempt")
                raise


def create_gold_schema(engine):
    """Create gold schema if it doesn't exist."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
        conn.commit()
    logger.info("Schema 'gold' verified/created successfully")


def drop_table_if_exists(engine, table_name):
    """Drop table if it exists."""
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS gold.{table_name} CASCADE"))
        conn.commit()
    logger.info(f"Gold table.{table_name} dropped (if existed)")


def create_gold_table(engine, table_name, sql):
    full_sql = f"""
    CREATE TABLE IF NOT EXISTS gold.{table_name} AS
    {sql}
    """
    
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS gold.{table_name}"))
        conn.execute(text(full_sql))
        conn.commit()
    
    logger.info(f"gold.{table_name} created successfully")


# ─────────────────────────────────────────────
# DASHBOARD 1: The great decoupling
# ─────────────────────────────────────────────

def create_dashboard_1_the_great_decoupling(engine):
    """
    Comparing the growth rate of GDP (Gross Domestic Product) over
    Total emissions for Brazil in last 30 years
    """
    
    sql = """
    SELECT * FROM co2_project_marts.great_decoupling_metrics 
    """
    
    create_gold_table(engine, "the_great_decoupling", sql)


# ─────────────────────────────────────────────
# DASHBOARD 2: Carbon Leakage
# ─────────────────────────────────────────────

def create_dashboard_2_carbon_leakage(engine):
    """
    Comparing Co2 Emissions against CO2 consumption for High-income countries since 1990
    """
    
    sql = """
    SELECT * FROM co2_project_marts.carbon_leakage_metrics"""
    
    create_gold_table(engine, "carbon_leakage", sql)


# ─────────────────────────────────────────────
# DASHBOARD 3: The amazon effect
# ─────────────────────────────────────────────

def create_dashboard_3_amazon_effect(engine):
    """
    See how the land use in Brazil impact in the total greenhouse gas emissions.
    """
    
    sql = """
    SELECT * FROM co2_project_marts.amazon_effect_metric
    """
    
    create_gold_table(engine, "amazon_effect", sql)


# ─────────────────────────────────────────────
# DASHBOARD 4: South America Leaderboard
# ─────────────────────────────────────────────

def create_dashboard_4_south_america_leaderboard(engine):
    """
    Ranking of emissions in South America
    """
    
    sql = """
    SELECT * FROM co2_project_marts.south_america_metrics
    """
    
    create_gold_table(engine, "south_america_leaderboard", sql)


# ─────────────────────────────────────────────
# DASHBOARD 5: The Hidden Polluters
# ─────────────────────────────────────────────

def create_dashboard_5_hidden_polluters(engine):
    """
    Identify top 5 countries where Nitrous Oxide (N2O) represents the highest percentage of their total GHG footprint in the last 20 years
    """
    
    sql = """
    SELECT * FROM co2_project_marts.hidden_polluters_metrics
    """
    
    create_gold_table(engine, "hidden_polluters", sql)


# ─────────────────────────────────────────────
# DASHBOARD 6: "Coal" to "Gas" Transition
# ─────────────────────────────────────────────

def create_dashboard_6_coal_to_gas_transition(engine):
    """
    Vizualise de Coal to Gas transition in Chile since 1990
    """
    
    sql = """
    SELECT * FROM co2_project_marts.comparisons_metrics
    """
    
    create_gold_table(engine, "coal_to_gas_transition", sql)


# ─────────────────────────────────────────────
# DASHBOARD 7: Temperature Change
# ─────────────────────────────────────────────

def create_dashboard_7_temperature_change(engine):
    """
    Visualization of the effect of the cumulative CO2 emission into Temperature change in degres Celsius
    """
    sql = """
    SELECT * FROM co2_project_marts.temperature_change_metrics
    """

    create_gold_table(engine, "temperature_change", sql)

# ─────────────────────────────────────────────
# Orquestração de Dashboards Metabase
# ─────────────────────────────────────────────

def wait_metabase(host, max_retries=20, delay=5):
    
    urls = [f"{host}/api/health", f"{host}/api/session/properties"]

    for i in range(max_retries):
        for url in urls:
            try:
                r = requests.get(url, timeout=3)
                if r.status_code == 200:
                    return True
            except:
                pass

        time.sleep(delay)

    return False


def setup_metabase_dashboards():
    """
    Automatically create dashboards on Metabase
    based on the gold tabels.
    """
    
    logger.info("\n" + "=" * 70)
    logger.info("Initiating Dashboards creation on Metabase")
    logger.info("=" * 70)
    
    try:
        # Verifies availability
        if not wait_metabase(METABASE_HOST):
            logger.warning("Metabase not ready - skipping dashboards")
            return
        # Autentica com Metabase
        api = MetabaseAPI(METABASE_HOST, METABASE_USER, METABASE_PASSWORD)

        # Ensure Metabase is configured before attempting login
        if not api.first_time_setup():
            logger.error("Metabase setup failed")
            return False
        
        if not api.authenticate():
            logger.warning("Metabase unavailable - skipping dashboard creation")
            return False
        
        # Connect the database if not already present
        db_id = api.get_db_id(POSTGRES_DB)
        if not db_id:
            logger.info(f"Database '{POSTGRES_DB}' not found — connecting it to Metabase...")
            db_data = api.add_database(
                POSTGRES_DB,
                POSTGRES_HOST,
                POSTGRES_PORT,
                POSTGRES_USER,
                POSTGRES_PASSWORD
            )
            if not db_data:
                logger.error("Failed to connect database to Metabase")
                return False
            db_id = db_data["id"]

        logger.info(f"Database found (ID: {db_id})")

        # Sync schema so tables become available
        api.sync_database(db_id)
        
        # Define configuração dos dashboards
        dashboards_config = {
            "the_great_decoupling": {
                "title": "The Great Decoupling",
                "description": "Comparing the growth rate of GDP over Total emissions for Brazil in last 30 Years",
                "display_type": "line",
                "visualization_settings": {
                    "graph.dimensions": ["year"],
                    "graph.metrics": ["ghg_intensity"]
                }
            },
            "carbon_leakage": {
                "title": "Carbon Leakage",
                "description": "Comparing Co2 Emissions against CO2 consumption for High-income countries since 1990",
                "display_type": "scatter",
                "visualization_settings": {
                    "graph.dimensions": ["country"],
                    "graph.metrics": ["carbon_gap"],
                    "scatter.bubble": "year"
                }
            },
            "amazon_effect": {
                "title": "Amazon Effect",
                "description": "See how the land use in Brazil impacts total greenhouse gas emissions",
                "display_type": "line",
                "visualization_settings": {
                    "graph.dimensions": ["year"],
                    "graph.metrics": ["land_use_over_ghg_prct"]
                }
            },
            "south_america_leaderboard": {
                "title": "South America Leaderboard of Emissions",
                "description": "Leaderboard of Co2 emissions in Latin America since beginning of 21st century",
                "display_type": "row",
                "visualization_settings": {
                    "graph.dimensions": ["country"],
                    "graph.metrics": ["cumulative_intensity"]
                }
            },
            "hidden_polluters": {
                "title": "Hidden Polluters",
                "description": "Top 5 countries where N2O represents the highest percentage of their total GHG footprint",
                "display_type": "bar",
                "visualization_settings": {
                    "graph.dimensions": ["country"],
                    "graph.metrics": ["hidden_impact"]
                }
            },
            "coal_to_gas_transition": {
                "title": "Coal to Gas Transition",
                "description": "Visualization of Coal to Gas Transition in Chile since 1990",
                "display_type": "line",
                "visualization_settings": {
                    "graph.dimensions": ["year"],
                    "graph.metrics": ["coal_co2_mt", "gas_co2_mt"]
                }
            },
            "temperature_change": {
                "title": "Temperature Change",
                "description": "Effect of cumulative CO2 emissions on Temperature change in Celsius",
                "display_type": "scatter",
                "visualization_settings": {
                    "graph.dimensions": ["year"],
                    "graph.metrics": ["temperature_change_from_co2_degrees_c"]
                }
            }
        }
        
        # Cria dashboard principal
        main_dash = api.create_dashboard(
            "Business Intelligence",
            "Centralized dashboard com strategical analyses"
        )
        
        if not main_dash:
            logger.error("Failure to create main dashboard")
            return False
        
        main_dash_id = main_dash["id"]
        logger.info(f"Main Dashboard created (ID: {main_dash_id})")
        
        # Cria cards e adiciona ao dashboard
        current_row = 0
        for table_name, config in dashboards_config.items():
            logger.info(f"\n→ Processando: {config['title']}")
            
            # Obtém ID da tabela
            table_id = api.get_table_id(db_id, METABASE_SCHEMA, table_name)
            if not table_id:
                logger.warning(f"Tabel no found: {table_name}")
                continue
            
            logger.info(f"Table found (ID: {table_id})")
            
            # Cria card
            card = api.create_card(db_id, table_id, config)
            if not card:
                logger.warning(f"Failure to create card {table_name}")
                continue
            
            # Adiciona card ao dashboard
            if api.add_card_to_dashboard(main_dash_id, card["id"], current_row):
                current_row += 8
            else:
                logger.warning(f"Failure to add card to dashboard")
        
        # Publica dashboard
        if api.publish_dashboard(main_dash_id):
            logger.info(f"\nDashboard published successfully!")
        
        return True
        
    except Exception as e:
        logger.error(f"Error to setup Metabase: {e}", exc_info=True)
        return False


# ─────────────────────────────────────────────
# ORQUESTRAÇÃO PRINCIPAL
# ─────────────────────────────────────────────

def metabase_pipeline():
    """Execute Gold pipeline."""
    
    try:
        logger.info("=" * 70)
        logger.info("Initiating Gold")
        logger.info("=" * 70)
        
        engine = get_engine()
        
        # Verifica conexão
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            conn.commit()
        logger.info("Database connection established")
        
        # Cria schema gold
        create_gold_schema(engine)
        
        # ─ DASHBOARDS
        logger.info("\n[1/7] Dashboard 1: The great decoupling...")
        drop_table_if_exists(engine, "the_great_decoupling")
        create_dashboard_1_the_great_decoupling(engine)
        
        logger.info("[2/7] Dashboard 2: Carbon leakage...")
        drop_table_if_exists(engine, "carbon_leakage")
        create_dashboard_2_carbon_leakage(engine)
        
        logger.info("[3/7] Dashboard 3: Amazon Effect...")
        drop_table_if_exists(engine, "amazon_effect")
        create_dashboard_3_amazon_effect(engine)
        
        logger.info("[4/7] Dashboard 4: South America leaderboard...")
        drop_table_if_exists(engine, "south_america_leaderboard")
        create_dashboard_4_south_america_leaderboard(engine)
        
        logger.info("[5/7] Dashboard 5: Hidden polluters...")
        drop_table_if_exists(engine, "hidden_polluters")
        create_dashboard_5_hidden_polluters(engine)
        
        logger.info("[6/7] Dashboard 6: Coal to Gas transition...")
        drop_table_if_exists(engine, "coal_to_gas_transition")
        create_dashboard_6_coal_to_gas_transition(engine)

        logger.info("[7/7] Dashboard 7: Temperature change...")
        drop_table_if_exists(engine, "temperature_change")
        create_dashboard_7_temperature_change(engine)
        
        # ─ Verifica dados criados
        logger.info("\n" + "─" * 70)
        logger.info("Verificando dados criados na camada Gold:")
        logger.info("─" * 70)
        
        dashboards = [
            "the_great_decoupling",
            "carbon_leakage",
            "amazon_effect",
            "south_america_leaderboard",
            "hidden_polluters",
            "coal_to_gas_transition",
            "temperature_change"
        ]
        
        logger.info("\n" + "=" * 70)
        logger.info("Pipeline Gold (tabels) concluded successfully!")
        logger.info("=" * 70)
        logger.info("\nDashboards created in Gold layer:")
        for table in dashboards:
            logger.info(f"  - gold.{table}")
        
        # ─ SETUP METABASE
        logger.info("\n" + "=" * 70)
        setup_metabase_dashboards()
        logger.info("=" * 70)
        
        logger.info("\nPIPELINE GOLD FINALIZED SUCCESSFULLY!")
        
    except Exception as e:
        logger.error(f"Error Gold Pipeline: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    metabase_pipeline()