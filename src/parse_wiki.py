import re
import os 
import gzip
import json
from tqdm import tqdm
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk import tokenize, word_tokenize, sent_tokenize

import utils 


def parse_page(pg_content):
    """
    Parse a specific wiki pages

    Parameters:
    -----------
        pg_content: dict
            Text Contents of page for specific taxonomy ID

    Returns:
    --------
    list
        Hierarchy of information (paragraph, sentences, words) present in page
    """
    soup = BeautifulSoup(pg_content, "lxml")
    main_div = soup.find_all('div', {'class': "mw-parser-output"})[0]
    paragraphs = main_div.find_all("p", recursive=False)

    strs_to_replace = [",", "'", "(", ")", "[", "]", '"', '.']

    # Each element is a paragraph. Which in turn, is a list where each element is a sentence
    parsed_paragraphs = []

    # Skip first...always empty
    for par in paragraphs[1:]:
        parsed_sentences = []  # List sentences for paragraph

        # Remove footnotes
        par_text = re.sub("\[.*?\]", "", par.text)
        par_sentences = sent_tokenize(par_text)

        # Remove Crud from words
        for sentence in par_sentences:
            words_in_sentence = []

            for w in sentence.split():

                # TODO: Is numeric this needed?
                if w != '' and not w.isnumeric():
                    for s in strs_to_replace:
                        w = w.replace(s, "")

                    words_in_sentence.append(w)
                
            parsed_sentences.append(words_in_sentence)
        
        # print(parsed_sentences)
        parsed_paragraphs.append(parsed_sentences)
    
    # exit()

    return parsed_paragraphs

    


def parse_all_pages():
    """
    Parse all the pages in the data/wiki_pgs directory

    Returns:
    --------
    None
    """
    tax2pg = utils.get_tax_2_pg_map() 

    for tax_id in tqdm(tax2pg, desc="Parsing Wiki Pages"):
        file_path = os.path.join(utils.DATA_DIR, "wiki_pgs", f"{tax_id}.json.gz")

        if tax2pg[tax_id] != -1:
            with gzip.open(file_path, 'rt', encoding='UTF-8') as zf:
                tax_pg = json.load(zf)
                tax_pg_content = tax_pg['parse']['text']["*"]
            
            parse_page(tax_pg_content)


def main():
    parse_all_pages()


if __name__ == "__main__":
    main()
