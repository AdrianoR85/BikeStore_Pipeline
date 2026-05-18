import sys
import os
from pathlib import Path

# Get the current user home path
user_home = os.path.expanduser("~")
sys.path.append(f"{user_home}/Project_bikestore")

from config import CRM

PATHS = {
    "crm_customer": f"{CRM['cst_info_path']}{CRM['cst_info_file']}",
    "crm_product": f"{CRM['prd_info_path']}{CRM['prd_info_file']}",
    "crm_sales": f"{CRM['sales_details_path']}{CRM['sales_details_file']}"
}