import os 
import gzip
import json
import requests
import pandas as pd
from tqdm import tqdm

import utils



def construct_tax_2_page_map(tax_ids, page_ids):
    """
    Create a save a mapping of Taxonomy IDs -> Wiki Page IDs

    When no corresponding page ID it's saved as -1

    Parameters:
    -----------
        tax_ids: list
            NCBI taxonomy IDs
        page_ids: list
            list of wiki page IDs
    
    Returns:
    --------
    dict
        tax_id -> page_id
    """
    tax_pg_map = {}

    for t, p in zip(tax_ids, page_ids):
        tax_pg_map[t] = p 
    
    with open(os.path.join(utils.DATA_DIR, "tax2pg.json"), "w") as f:
        json.dump(tax_pg_map, f, indent=4)
    
    return tax_pg_map


def get_all_children(df, top_parent_id):
    """
    For some ID we work our way down and get all of its children.

    Starting from initial id "top_parent_id":
        - Add to cur_ids
        - Pop top id in cur_ids
        - Get all children (where 'patent_tax_id' == id)
        - Add childrent to cur_ids list 
        - Add current id to 'final_ids'

    Parameters:
    -----------
        df: pd.DataFrame
            NCBI Taxonomy df
        top_parent_id: int
            ID we want to get all children under
    
    Returns:
    --------
    list
        All children IDs
    """
    final_ids = []
    cur_ids = [top_parent_id]

    print("Getting species IDs...", end="")

    while cur_ids:
        pid = cur_ids.pop()
        child_df = df[df['parent_tax_id'] == pid]
        child_ids = child_df['tax_id'].values.tolist()

        # If it has children we still add children to list to search
        # Otherwise it's a species_ids and we add it to the final species_ids list
        if child_ids:
            # print(f"ID {pid} - {len(child_ids)} children")
            cur_ids.extend(child_ids)
        
        final_ids.append(pid)

    print(f"{len(final_ids)} IDs")

    return final_ids


def search_for_wiki_pg(keyword):
    """
    Search for a wiki pg corresponding to a single keyword

    Example Page URL: https://en.wikipedia.org/w/api.php?action=query&format=json&list=search&srwhat=nearmatch&srsearch=Solanoideae

    Parameters:
    -----------
        keyword: str
            Name of plant or some sort of taxonomy hierarchy
    
    Returns:
    --------
    int
        page id. -1 if it doesn't exist
    """
    base_url = "https://en.wikipedia.org/w/api.php?action=query&format=json&list=search&srwhat=nearmatch&srsearch="

    search_param = "+".join(keyword.split())
    search_url = base_url + search_param

    r = requests.get(search_url).json()
    search_results = r['query']['search']

    if search_results:
        pid = search_results[0]['pageid']
    else:
        pid = -1
    
    return pid


def search_wiki_pgs(plant_names, tax_ids):
    """
    For a list of names. Get the corresponding wikipedia page ids for them if they exist.

    We use the page ids saved in tax2pg.json if it exists to save time.

    Parameters:
    -----------
        plant_names: list
            list of names of plant names
        tax_ids: list
            list of corresponding NCBI tax ids for names
    
    Returns:
    --------
    list
        corresponding page ids for plant names
    """
    page_ids = []
    tax2pg_id = utils.get_tax_2_pg_map()

    for pname, tid in tqdm(zip(plant_names, tax_ids), desc="Searching for Wiki Pgs", total=len(plant_names)):
        tid = str(tid)

        # no need to query when it's already saved
        if tid in tax2pg_id:
            pid = tax2pg_id[tid]
        else:
            pid = search_for_wiki_pg(pname)

        page_ids.append(pid)

    return page_ids


def get_wiki_pgs(tax2pg):
    """
    Get wiki pages for corresponding tax/page IDs when it's not already saved to disk

    Save pages to ./data/wiki_pgs directiory

    Example Page URL: https://en.wikipedia.org/w/api.php?action=parse&format=json&pageid=47862508

    Parameters:
    -----------
        tax2pg: dict
            Maps tax id to wiki page id 
    
    Returns:
    --------
    None
    """
    base_url = "https://en.wikipedia.org/w/api.php?action=parse&format=json&pageid="

    for tax_id, page_id in tqdm(tax2pg.items(), desc="Retrieving Wiki Pgs"):
        page_url = f"{base_url}{page_id}"
        file_path = os.path.join(utils.DATA_DIR, "wiki_pgs", f"{tax_id}.json.gz")

        # 1. Page Id = -1 means doesn't exists
        # 2. If file exists no need to fetch again
        if page_id != -1 and not os.path.isfile(file_path):
            r = requests.get(page_url).json()
        
            with gzip.open(file_path, 'wt', encoding='UTF-8') as zf:
                json.dump(r, zf)



def construct_plant_df():
    """
    Construct the initial plant dataframe from the ncbi_tax.csv file

    Returns:
    --------
    pd.DataFrame 
        DataFrame of plant info
    """
    plant_df = pd.read_csv(os.path.join(utils.DATA_DIR, "ncbi_tax.csv"))

    # NOTE: We start from here for testing!
    parent_id = plant_df[plant_df['name'] == "Solanoideae"].iloc[0]['tax_id']

    plant_ids = get_all_children(plant_df, parent_id)
    plant_child_df = plant_df[plant_df['tax_id'].isin(plant_ids)]

    return plant_child_df


def main():
    plant_df = construct_plant_df()

    plant_names = plant_df['name'].values.tolist()
    tax_ids = plant_df['tax_id'].values.tolist()

    pg_ids = search_wiki_pgs(plant_names, tax_ids)
    tax2pg = construct_tax_2_page_map(tax_ids, pg_ids)

    get_wiki_pgs(tax2pg)


if __name__ == "__main__":
    main()

