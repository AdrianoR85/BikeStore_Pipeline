import sys
import os
from pathlib import Path

# Get the current user home path
user_home = os.path.expanduser("~")
sys.path.append(f"{user_home}/Project_bikestore")

from config import ERP

PATHS = {
    "erp_customer": f"{ERP['cust_az12_path']}{ERP['cust_az12_file']}",
    "erp_location": f"{ERP['loc_a101_path']}{ERP['loc_a101_file']}",
    "erp_category": f"{ERP['px_cat_g1v2_path']}{ERP['px_cat_g1v2_file']}"
}