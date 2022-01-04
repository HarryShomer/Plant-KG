import os 
import json 

DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")


def get_tax_2_pg_map():
    """
    Read from disk if there. Otherwise return empty dict
    
    Returns:
    --------
    dict
        tax_id -> page_id
    """
    file_name = os.path.join(DATA_DIR, "tax2pg.json")

    if os.path.isfile(file_name):
        with open(file_name, "r") as f:
            tax2pg = json.load(f)
    else:
        tax2pg = {}
    
    return tax2pg
