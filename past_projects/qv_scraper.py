import time
import os
import requests
from bs4 import BeautifulSoup
import csv
import itertools

import random
from urllib.parse import urlparse

from multiprocessing import Pool

def GET_UA():
    uastrings = ["Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36",\
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0",\
                "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/600.1.17 (KHTML, like Gecko) Version/7.1 Safari/537.85.10",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",\
                "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36"\
                ]
 
    return random.choice(uastrings)


def scrape(count):
    """ Navigates to quotablevalue.co.nz and retrieves all of the property info on each house in their database.
        returns a dictionary containing houses' info."""

    out_dict = {}
    it = 0
    while True:
        house_details = {}
        page = requests.get(f"https://quotablevalue.co.nz/property-search/property-details/{count}/", headers={'User-Agent': GET_UA()})
        soup = BeautifulSoup(page.content, 'html.parser')
        address = soup.find(id="property-details-address")

        # Tries to find the name of the home.
        # try:
        #     address.text
        # except:

        #     count += 1
        #     it += 1
        #     continue

        dl_data = soup.find_all("dd")

        # Below finds the respective information about each home.
        capital_value = dl_data[0].text
        house_details['capital_value'] = capital_value

        land_value = dl_data[1].text
        house_details['land_value'] = land_value

        last_valued = dl_data[2].text
        house_details['last_valued'] = last_valued

        improvement_value = dl_data[3].text
        house_details['improvement_value'] = improvement_value

        valuation_reference = dl_data[4].text
        house_details['valuation_reference'] = valuation_reference

        legal_description = dl_data[5].text
        house_details['legal_description'] = legal_description

        out_dict[address.text] = house_details

        print(out_dict)

        # This function ends after 1000 iterations in order to produce multiple files.
        # Also allows the function to be easily rerun at some interval if a crash happens.
        if it == 1000:
            print(f'{count} houses done.')
            break

        else:
            count += 1
            it += 1

    # Here out_dict is a nested dictionary, where each house has the info extracted above in a dict.
    return out_dict

def save_data(in_dict, path, iterations):
    """ Checks the target directory, expects input dictionary of house information, saves
        a new file, with filename changing as per first line of code below."""

    file = os.path.join(path, f'qv_housing_{iterations}.csv')
    fields = ['address', 'capital_value', 'land_value', 'last_valued', 'improvement_value', 'valuation_reference', 'legal_description']
    
    # If the file already exists, append to it;
    if os.path.exists(file):
        with open(file, "a") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            for k,d in (in_dict.items()):
                w.writerow(mergedict({'address': k},d))

    # Else write the file.
    else:
        with open(file, "w") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for k,d in (in_dict.items()):
                w.writerow(mergedict({'address': k},d))
            
    print(f'Wrote file to {file}')
        

def mergedict(a,b):
    """ Helper function to convert nested dictionary to .csv easier."""
    a.update(b)
    return a


def main():
    """ Runs scraper per iteration count until all data from website is scraped, saves to 
        house_data_dir a file for when scraper count reaches 1000, i.e. every 1000 houses,
        makes a new .csv file."""
    house_data_dir = r'/mnt/temp/sync_to_data/LINZ/fixed_housing_data'
    
    # iterations = 505
    print('Starting web scraping...')

    x = 1748
 
    while True:
        try:
            with Pool(4) as p:
                df_list = p.map(scrape, [x*1000, ((x+1)*1000), ((x+2)*1000), ((x+3)*1000)])

            with Pool(4) as p1:
                args = [(df_list[0], house_data_dir, (x*1000)), (df_list[1], house_data_dir, ((x+1)*1000)), 
                        (df_list[2], house_data_dir, ((x+2)*1000)), (df_list[3], house_data_dir, ((x+3)*1000))]
                p1.starmap(save_data, args)

            x += 4

            print(f'Currently on iteration x = {x * 1000}...')
        except:
            print(f'Skipped iteration..')
            x+=4
            continue

main()
