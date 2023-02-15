#!/usr/bin/env python3
# bgc_modified_uploader.py
"""
Script to upload BGC data into Benchling results tables.
"""
import argparse
import json
import os
import pandas
import re
import requests
import sys
import numpy
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from ratelimiter import RateLimiter


def search_benchling(auth_key, api_url, solarea_name):
    """Return json data using a API endpoint URL and the name of the entity you want info for"""
    auth_key='sk_CGOJHeOIwLNpOq3BGmqVaLRpFXoFL'
    # full API endpoint for searching for an entity by name
    link_search = api_url + solarea_name
    # retrieve the custom entity with the specified name
    search_results = requests.get(link_search, auth=HTTPBasicAuth(auth_key, ''))
    # Save the retrieved entity data
    search_json = search_results.json()
    # if the request fails, print the error message
    if search_results.status_code != 200:
        print(link_search)
        print(json.dumps(search_json, sort_keys=True, indent=4))
        sys.exit(f"Failed to find Benchling ID for {solarea_name}.\nHTTP status code: {search_results.status_code}")
    return search_json


def create_df(tsv_file, auth_key, api_url):
    """Creates a dataframe using output from the BGC pipeline"""
    bgc_df = pandas.read_csv(tsv_file, header = 0, index_col = False)
    bgc_df = bgc_df.replace(numpy.nan, '', regex=True)
    # Set rate limiter
    rate_limiter = RateLimiter(max_calls=2, period=1)
    # create individual dataframes for each of the BGC tools and obtain the benchling dropdown URL
    dropdown_url = 'https://solareabio.benchling.com/api/v2/dropdowns/'

    # Obtain entity id from benchling using rate limiter 
    entity_ids = []
    solarea_ids = set()
    benchling_ids = []
    for row in bgc_df.loc[:, 'SBI ID']:
        solarea_ids.add(row)
    for entity in tqdm(solarea_ids):
        with rate_limiter: 
            entity_data = search_benchling(auth_key, api_url, entity)
        try:
            entity_id = entity_data["customEntities"][0]['id']
            benchling_ids.append(entity_id)
        except IndexError:
            sys.exit(f'Could not find {entity}')
    entity_dict = dict(zip(solarea_ids, benchling_ids))
    for row in bgc_df.loc[:, 'SBI ID']:
        if row in entity_dict:
            entity_ids.append(entity_dict[row])
    bgc_df.loc[:, 'SBI ID'] = entity_ids

    grouped = bgc_df.groupby('tool')
    antismash_df = grouped.get_group('antismash')
    deepbgc_df = grouped.get_group('deepbgc')
    bagel_df = grouped.get_group('BAGEL')

    # obtain dropdown entities for antismash categories and products from benchling
    products = []
    categories = []
    category_ids = []
    category_names = []
    product_ids = []
    product_names = []
    antismash_category = search_benchling(auth_key, dropdown_url, 'sfs_PKplVIs3')
    antismash_product = search_benchling(auth_key, dropdown_url, 'sfs_tPkioojn')
    for i in range(len(antismash_category["options"])):
        category_ids.append(antismash_category["options"][i]["id"])
        category_names.append(antismash_category["options"][i]["name"])
    category_dict = dict(zip(category_names, category_ids))
    for row in antismash_df.loc[:, 'Category']:
        row_data = []
        if row:
            category = category_dict[row]
            row_data.append(category)
        else:
            row_data.append(None)
        categories.append(row_data)
    antismash_df.loc[:, 'Category'] = categories
    for i in range(len(antismash_product["options"])):
        product_ids.append(antismash_product["options"][i]["id"])
        product_names.append(antismash_product["options"][i]["name"])
    product_dict = dict(zip(product_names, product_ids))
    for row in antismash_df.loc[:, 'Product']:
        row_data = []
        if row:
            product = product_dict[row]
            row_data.append(product)
        else:
            row_data.append(None)
        products.append(row_data)
    antismash_df.loc[:, 'Product'] = products

    # Obtain dropdown entities for deepbgc product class and product activities from benchling
    product_classes = []
    # entity_ids = []
    product_activities = []
    product_activity_ids = []
    product_activity_names = []
    product_class_ids = []
    product_class_names = []
    deepbgc_product_class = search_benchling(auth_key, dropdown_url, 'sfs_JbfzDXHP')
    deepbgc_product_activity = search_benchling(auth_key, dropdown_url, 'sfs_U1peTktq')
    for i in range(len(deepbgc_product_class["options"])):
        product_class_ids.append(deepbgc_product_class["options"][i]["id"])
        product_class_names.append(deepbgc_product_class["options"][i]["name"])
    product_class_dict = dict(zip(product_class_names, product_class_ids))
    for row in deepbgc_df.loc[:, 'product_class']:
        row_data = []
        if row:
            deepbgc_class = product_class_dict[row]
            row_data.append(deepbgc_class)
        else:
            row_data.append(None)
        product_classes.append(row_data)
    deepbgc_df.loc[:, 'product_class'] = product_classes
    for i in range(len(deepbgc_product_activity["options"])):
        product_activity_ids.append(deepbgc_product_activity["options"][i]["id"])
        product_activity_names.append(deepbgc_product_activity["options"][i]["name"])
    product_activity_dict = dict(zip(product_activity_names, product_activity_ids))
    for row in deepbgc_df.loc[:, 'product_activity']:
        row_data = []
        if row:
            deepbgc_activity = product_activity_dict[row]
            row_data.append(deepbgc_activity)
        else:
            row_data.append(None)
        product_activities.append(row_data)
    deepbgc_df.loc[:, 'product_activity'] = product_activities

    # Obtain dropdown entities for bagel product classes from benchling 
    # product_classes = []
    classes = []
    class_ids = []
    class_names = []
    bagel_products = search_benchling(auth_key, dropdown_url, 'sfs_LitnMMw8')
    for i in range(len(bagel_products["options"])):
        class_ids.append(bagel_products["options"][i]["id"])
        class_names.append(bagel_products["options"][i]["name"])
    bagel_class_dict = dict(zip(class_names, class_ids))
    for row in bagel_df.loc[:, 'product_class']:
        row_data = []
        if row:
            clear_space = row.rstrip()
            bagel_class = bagel_class_dict[clear_space]
            row_data.append(bagel_class)
        else:
            row_data.append(None)
        classes.append(row_data)
    bagel_df.loc[:, 'product_class'] = classes
    return antismash_df, deepbgc_df, bagel_df

# Create schemas for each tool 
def create_antismash_schema(dataframe):
    """Create a json object for antismash output from BGC pipeline"""
    return {
        "assayResults": [
            {
                "fields": {
                    "entity": {"value": j['SBI ID']},
                    "sequence_id": {"value": j['sequence_id']},
                    "category": {"value": j['Category']},
                    "product": {"value": j['Product']},
                    "start": {"value": j['start']},
                    "end": {"value": j['end']},
                    "nucleotide_length": {"value": j['nucl_length']}
                },
                "schemaId": "assaysch_Z6B7TlWG",
                "projectId": 'src_tIUAcxDH'
            } for i, j in dataframe.iterrows()
        ]
    }

def create_deepbgc_schema(dataframe):
    """Create a json object for deepbgc output from BGC pipeline"""
    return {
        "assayResults": [
            {
                "fields": {
                    "entity": {"value": j['SBI ID']},
                    # "sequence_id": {"value": j['sequence_id']},
                    "start": {"value": j['start']},
                    "end": {"value": j['end']},
                    "nucleotide_length": {"value": j['nucl_length']},
                    "num_proteins": {"value": j['num_proteins']},
                    "product_activity": {"value": j['product_activity']},
                    "product_class": {"value": j['product_class']},
                    "score": {"value": j['deepbgc_score']}
                },
                "schemaId": "assaysch_RDNox02d",
                "projectId": 'src_tIUAcxDH'
            } for i, j in dataframe.iterrows()
        ]
    }


def create_bagel_schema(dataframe):
    """Create a json object for BAGEL output from BGC pipeline"""
    return {
        "assayResults": [
            {
                "fields": {
                    "entity": {"value": j['SBI ID']},
                    "start": {"value": j['start']},
                    "end": {"value": j['end']},
                    "product_class": {"value": j['product_class']}
                },
                "schemaId": "assaysch_7aLSUwS2",
                "projectId": 'src_tIUAcxDH'
            } for i, j in dataframe.iterrows()
        ]
    }


def get_cli_args():
    """Set command line options"""
    my_parser = argparse.ArgumentParser(description="Upload metagenomic analysis data to Benchling")
    my_parser.add_argument('-i', '--input', action='store', type=str,
                           help='Path to the directory with Metagenomes pipeline output data to upload', required=True)
    my_parser.add_argument('-k', '--apikey', action='store', type=str, help='Your Benchling API key', required=True)
    return my_parser.parse_args()


def main():
    args = get_cli_args()

    # URLs for Benchling API endpoints
    name_link = "https://solareabio.benchling.com/api/v2/custom-entities?pageSize=50&sort=name&name="
    # id_link = "https://solareabio.benchling.com/api/v2/custom-entities?pageSize=50&sort=modifiedAt%3Adesc&schemaId="
    upload_link = 'https://solareabio.benchling.com/api/v2/assay-results'

    n = 100  # chunk row size  # chunk row size

    antismash_table, deepbgc_table, bagel_table = create_df(args.input, args.apikey, name_link)
    # antismash
    # Split table into chunks for uploading
    antismash_list = [antismash_table[i: i + n] for i in range(0, antismash_table.shape[0], n)]
    deepbgc_list = [deepbgc_table[i: i + n] for i in range(0, deepbgc_table.shape[0], n)]
    bagel_list = [bagel_table[i: i + n] for i in range(0, bagel_table.shape[0], n)]
    # antismash_table.to_csv('antismash_table.csv')
    # deepbgc_table.to_csv('deepbgc_table.csv')
    # bagel_table.to_csv('bagel_table.csv')
    # Upload each chunk of the dataframe
    print("Uploading Antismash data to Benchling...")
    for df in tqdm(antismash_list):
        antismash_post = requests.post(upload_link, auth=HTTPBasicAuth(args.apikey, ''),
                                     json=create_antismash_schema(df))
        print(json.dumps(antismash_post.json(), sort_keys=True, indent=4))
        if antismash_post.status_code == 200:
            print("Uploaded successfully.")
        else:
            sys.exit(f"Failed to upload summary.\nHTTP status code: {antismash_post.status_code}")

    # deepbgc
    print("Uploading deepbgc data to Benchling...")
    for df in tqdm(deepbgc_list):
        deepbgc_post = requests.post(upload_link, auth=HTTPBasicAuth(args.apikey, ''),
                                     json=create_deepbgc_schema(df))
        print(json.dumps(deepbgc_post.json(), sort_keys=True, indent=4))
        if deepbgc_post.status_code == 200:
            print("Uploaded successfully.")
        else:
            sys.exit(f"Failed to upload summary.\nHTTP status code: {deepbgc_post.status_code}")
                
    # bagel
    print("Uploading bagel data to benchling")
    for df in tqdm(bagel_list):
        bagel_post = requests.post(upload_link, auth=HTTPBasicAuth(args.apikey, ''),
                                     json=create_bagel_schema(df))
        print(json.dumps(bagel_post.json(), sort_keys=True, indent=4))
        if bagel_post.status_code == 200:
            print("Uploaded successfully.")
        else:
            sys.exit(f"Failed to upload summary.\nHTTP status code: {bagel_post.status_code}")


if __name__ == '__main__':
    main()
