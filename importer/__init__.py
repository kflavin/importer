import os
__version = "0.12.10"

weekly_prefix = "npi-in/weekly"
monthly_prefix = "npi-in/monthly"
deactivated_prefix = "npi-in/deactivated"

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SP_DIR = os.path.join(ROOT_DIR, "sql/products/sp")
