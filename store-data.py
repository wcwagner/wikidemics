#!/usr/bin/env python

"""
Loads wikipedia page views and ILInet weekly ILI data from the DELPHI Epidata API.
This script serializes the API responses into the ./data folder via the pickle module.
For ease of use, the API responses are also stored into pandas DataFrames, which are also pickled.

The format of the pageviews_df is:

Epiweek(Index), Amatadine, ..., Influenza
200801          1024,      ..., 5424
...
201652          4385,      ..., 10905

The format of the wILI_df is:

Epiweek(Index), Weekly ILI
200801          1.423
...
201652          3.019

"""


import delphi_epidata as delphi
import pandas as pd
from collections import defaultdict
import pickle
import os

FLU_RELATED_ARTICLES = [
    'amantadine', 'antiviral_drugs', 'avian_influenza', 'canine_influenza', 'cat_flu', 'chills', 'common_cold', 'cough',
    'equine_influenza', 'fatigue_(medical)', 'fever', 'flu_season', 'gastroenteritis', 'headache',
    'hemagglutinin_(influenza)', 'human_flu', 'influenza', 'influenzalike_illness', 'influenzavirus_a',
    'influenzavirus_c', 'influenza_a_virus', 'influenza_a_virus_subtype_h10n7', 'influenza_a_virus_subtype_h1n1',
    'influenza_a_virus_subtype_h1n2', 'influenza_a_virus_subtype_h2n2', 'influenza_a_virus_subtype_h3n2',
    'influenza_a_virus_subtype_h3n8', 'influenza_a_virus_subtype_h5n1', 'influenza_a_virus_subtype_h7n2',
    'influenza_a_virus_subtype_h7n3', 'influenza_a_virus_subtype_h7n7', 'influenza_a_virus_subtype_h7n9',
    'influenza_a_virus_subtype_h9n2', 'influenza_b_virus', 'influenza_pandemic', 'influenza_prevention',
    'influenza_vaccine', 'malaise', 'myalgia', 'nasal_congestion', 'nausea', 'neuraminidase_inhibitor',
    'orthomyxoviridae', 'oseltamivir', 'paracetamol', 'rhinorrhea', 'rimantadine', 'shivering', 'sore_throat',
    'swine_influenza', 'viral_neuraminidase', 'viral_pneumonia', 'vomiting', 'zanamivir'
]
DATA_PATH = 'data/'
EPIDATA = delphi.Epidata()  # handle to the DELPHI epidata API


def get_wiki_pageviews(articles, epiweeks_rng):
    """Calls DELPHI epidata API for wikipedia page views on articles passed in."""
    try:
        pageviews = EPIDATA.wiki(articles, epiweeks=epiweeks_rng)['epidata']
        return pageviews
    except KeyError:
        print('Epidata API call failed to return any results')
        return


def create_pageviews_df(pageviews):
    """Creates a dataframe from the DELPHI epidata pageviews response"""
    epiweek_to_pageviews = defaultdict(list)
    pageviews_df = pd.DataFrame()
    for week in pageviews:
        epiweek, page, weekly_views = week['epiweek'], week['article'], week['count']
        epiweek_to_pageviews[epiweek].append((page, weekly_views))

    for epiweek in epiweek_to_pageviews:
        for page, weekly_views in epiweek_to_pageviews[epiweek]:
            pageviews_df.at[(epiweek, page)] = weekly_views
    pageviews_df = pageviews_df.fillna(pageviews_df.median())
    pageviews_df = pageviews_df.sort_index()
    return pageviews_df


def get_wILI(epiweeks_rng):
    """Calls DELPHI epidata API for CDC's weekly Influenza-like-illness for the range of epiweeks passed in"""
    try:
        wILI = EPIDATA.fluview('nat', epiweeks=epiweeks_rng)['epidata']
        return wILI
    except KeyError:
        print('Epidata API call failed to return any results')
        return


def create_wILI_df(wILI):
    """Creates a dataframe from the DELPHI epidata pageviews response"""
    wILI.sort(key=lambda d: d['epiweek'])
    wILI_values = [week['ili'] for week in wILI]
    wILI_index = [week['epiweek'] for week in wILI]
    wILI_df = pd.DataFrame(wILI_values, columns=['Weekly ILI'], index=wILI_index)
    return wILI_df


if __name__ == '__main__':
    pageviews = []
    pageviews_df = pd.DataFrame(columns=FLU_RELATED_ARTICLES)

    # API requests chunked into years due to size limitations
    for yr in range(2008, 2017):
        yr_start = int(str(yr) + '01')
        # 2008 and 2014 have 53rd epiweek
        yr_end = int(str(yr) + '53') if yr in [2008, 2014] else int(str(yr) + '52')
        yr_epiweek_range = EPIDATA.range(yr_start, yr_end)
        yr_pageviews = get_wiki_pageviews(FLU_RELATED_ARTICLES, epiweeks_rng=yr_epiweek_range)
        yr_pageviews_df = create_pageviews_df(yr_pageviews)
        # extend both data structures to include this year
        pageviews.extend(yr_pageviews)
        pageviews_df = pd.concat([pageviews_df, yr_pageviews_df])

    pageviews_df.sort_index()  # sort by epiweek
    with open(os.path.join(os.getcwd(), DATA_PATH, 'pageviews.pickle'), 'wb') as handle:
        pickle.dump(pageviews, handle, protocol=pickle.HIGHEST_PROTOCOL)
    with open(os.path.join(os.getcwd(), DATA_PATH, 'pageviews-df.pickle'), 'wb') as handle:
        pickle.dump(pageviews_df, handle, protocol=pickle.HIGHEST_PROTOCOL)

    wILI = get_wILI(epiweeks_rng=EPIDATA.range(200801, 201730))
    wILI_df = create_wILI_df(wILI)
    with open(os.path.join(os.getcwd(), DATA_PATH, 'wILI.pickle'), 'wb') as handle:
        pickle.dump(wILI, handle, protocol=pickle.HIGHEST_PROTOCOL)
    with open(os.path.join(os.getcwd(), DATA_PATH, 'wILI-df.pickle'), 'wb') as handle:
        pickle.dump(wILI_df, handle, protocol=pickle.HIGHEST_PROTOCOL)
