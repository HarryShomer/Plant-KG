import os 
import gzip
import tarfile
import requests
import pandas as pd

import utils



def retrieve_tax_gz():
    """
    Retrieve file from ncbi ftp site if it doesn't exists already.
    Once retrieved save file and unpack it into the data directory.

    URL - https://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz

    Returns:
    --------
        None
    """
    ncbi_data_dir = os.path.join(utils.DATA_DIR, "ncbi_taxonomy")
    file = os.path.join(ncbi_data_dir, "ncbi_taxdump.tar.gz")
    url = "https://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz"

    if not os.path.isdir(utils.DATA_DIR):
        os.mkdir(utils.DATA_DIR)

    if not os.path.isdir(ncbi_data_dir):
        os.mkdir(ncbi_data_dir)

    if not os.path.isfile(file):
        print(f"Retrieving {file} from {url}")
        r = requests.get(url)

        print(f"Writing file to disk")
        with gzip.open(file, 'wb') as f:
            f.write(gzip.decompress(r.content))

        print(f"Extracting {file}")
        with tarfile.open(file, "r:gz") as f:
            f.extractall(path=ncbi_data_dir)
    else:
        print(f"{file} is already on disk")


def construct_plant_df():
    """
    Construct a DataFrame containing information for all plants.

    Steps:
        - Filter `ncbi_taxonomy/nodes.dmp` for only plant species (division_id = 4)
        - Filter `ncbi_taxonomy/names.dmp` for rows that associated with a scientific name (name_class = 'scientific name')
        - Merge info from two files above on tax_id  (i.e. taxonomy id) 

    Returns:
    --------
    pd.DataFrame
        Merged DataFrame of plant info
    """
    names_cols = ['tax_id', 'name', 'unique_name', 'name_class']
    nodes_cols = ['tax_id', 'parent_tax_id', 'rank', 'embl_code', 'division_id', 'inherited_div', 'genetic_code_id', 
                  'inherited_gc', 'm_genetic_code_id', 'inherited_mgc', 'genbank', 'substree', 'comments']

    nodes_df = pd.read_csv(os.path.join(utils.DATA_DIR, "ncbi_taxonomy/nodes.dmp"), sep="\t\\|\t", header=None, names=nodes_cols)
    names_df = pd.read_csv(os.path.join(utils.DATA_DIR, "ncbi_taxonomy/names.dmp"), sep="\t\\|\t", header=None, names=names_cols)
    
    # Plants!
    plant_nodes_df = nodes_df[nodes_df['division_id'] == 4]

    # Remove 1st part of line terminator for both files
    nodes_df['comments'] = nodes_df['comments'].str.replace("\t\\|", "")
    names_df['name_class'] = names_df['name_class'].str.replace("\t\\|", "")

    # Few types of names...we just care for the official name
    scientific_names_df = names_df[names_df['name_class'] == "scientific name"]
    scientific_names_df = scientific_names_df.drop(columns=['unique_name'])

    return pd.merge(scientific_names_df, plant_nodes_df, on='tax_id')


def main():
    retrieve_tax_gz() 
    plant_df = construct_plant_df()
    plant_df.to_csv(os.path.join(utils.DATA_DIR, "ncbi_tax.csv"), index=False)


if __name__ == "__main__":
    main()
