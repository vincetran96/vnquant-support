'''Based on the industry classification API from VNDirect'''
# Basic query param structure:
#   :params:
#       q: the core part of the query; possible inputs:
#           - codeList: list of string; tickers to query
#           - industryCode: string; the industry code to query
#           - industryLevel: int or list of int; range from 1 to 3;
#               with 1 being the highest industry level and 3 the lowest
#           - higherLevelCode: string; the industry code of
#               the higher level to query
#           - englishName: part of the English name
#               of the industry to query
#           - vietnameseName: part of the Vietnamese name
#               of the industry to query
#       size: the number of "elements" in a query result page;
#           with an element representing an industry

# TODO: How to deal with a Vietnamese name query?

import random
import logging
import urllib.parse
from copy import deepcopy
from typing import List, Tuple, Literal
import httpx
import pandas
import vnquant.DataLoader as vnd_loader
from common.configs import USER_AGENTS


# Constants
# Susceptible to API change
CONTENT_TYPE = "application/json"
MAX_QUERY_SIZE = 9999
BASE_URL = "https://finfo-api.vndirect.com.vn/v4/industry_classification"
PAYLOAD_Q_KEYS = {
    'codeList': "",
    'industryCode': "",
    'industryLevel': "",
    'higherLevelCode': "",
    'englishName': "",
    'vietnameseName': ""
}
PAYLOAD_Q_JOIN_CHAR = "~"
BASE_PAYLOAD = {
    'q': "",
    'size': ""
}
PAYLOAD_SAFE_CHARS = ":," # Not to encode these in query param
DEFAULT_INDUSTRY_LEVEL = 1 # Default industry level

def get_ind_class(
        code_list: List[str]=None,
        industry_codes: List[str]=None,
        higher_level_codes: List[str]=None,
        english_name: str="",
        vietnamese_name: str="",
        result_size: int=MAX_QUERY_SIZE
    ) -> Tuple[pandas.DataFrame, dict]:
    '''Gets industries and their available tickers

    :params:
        @code_list: list of str - tickers
        @industry_codes: list of str - industry codes
        @industry_levels: list of str - industry levels
        @higher_level_codes: list of str - higher industry level's codes
        @english_name: str - part of the industry's English name to query for
        @vietnamese_name: str - part of the industry's Vietnamese name to query for
        @result_size: int - the number of industry to include on 1 result page

    :returns:
        - DataFrame: pandas DataFrame of industry classification
        - Metadata: metadata dictionary about the request
    '''

    # Prepare payload
    # Construct a single string containing all keys for the 'q' parameter

    # Parse the payload dict using the payload safe chars

    # Process the JSON response
    # Then put industry data from it into a DataFrame
    # Metadata is everything else other than industry data
    payload = deepcopy(BASE_PAYLOAD)
    payload_q_keys = deepcopy(PAYLOAD_Q_KEYS)
    payload_q_keys['industryLevel'] = DEFAULT_INDUSTRY_LEVEL
    if code_list:
        payload_q_keys['codeList'] = ",".join(code_list)
    if industry_codes:
        payload_q_keys['industryCode'] = ",".join(industry_codes)
    if higher_level_codes:
        payload_q_keys['higherLevelCode'] = ",".join(higher_level_codes)
    payload_q_keys['englishName'] = english_name
    payload_q_keys['englishName'] = vietnamese_name
    payload_q_str = PAYLOAD_Q_JOIN_CHAR.join(
        [f"{key}:{value}" for key, value in payload_q_keys.items()]
    )
    payload['q'] = payload_q_str
    payload['size'] = result_size

    payload_str = urllib.parse.urlencode(payload, safe=PAYLOAD_SAFE_CHARS)
    headers = {
        'content-type': CONTENT_TYPE,
        'User-Agent': random.choice(USER_AGENTS)
    }
    resp = httpx.get(
        BASE_URL,
        params=payload_str,
        headers=headers
    )

    resp_json = resp.json()
    industry_df = pandas.DataFrame(resp_json['data'])
    metadata_dict = {key: value for key, value in resp_json.items() if key != 'data'}

    return industry_df, metadata_dict


def get_full_ind_class():
    '''Gets full industry classification on VNDirect
    '''

    return get_ind_class()

def get_price_from_ind_df(
        industry_df: pandas.DataFrame,
        start: str,
        end: str,
        minimal: bool=True,
        data_source: Literal["vnd", "cafe"]="vnd"
    ) -> pandas.DataFrame:
    '''Gets stock price data from
    a industry classification DataFrame

    :params:
        @industry_df: a pandas DataFrame returned from the get_ind_class function;
            it must at least have a `codeList` column where
            its value is a string of stock codes (e.g., "AAA,HCM")
        @start: str - start date of the period to get price for;
            must be in format %Y-%m-%d as per strftime
        @end: str - end date of the period to get price for;
            must be in format %Y-%m-%d as per strftime
        @minimal: bool - whether to get minimal price information;
            if False, get more than
            just high, low, open, close, adjust price, volume
        @data_source: str - source to download stock price;
            options are vnd (for VNDirect) or cafe (for CafeF)
    '''

    # Get a list of strings of stock codes from the
    #   `codeList` column of the provided DataFrame
    # Then pass that list into the DataLoader object
    code_list = ",".join(list(industry_df['codeList'])).split(",")
    loader = vnd_loader.DataLoader(
        symbols=code_list,
        start=start,
        end=end,
        minimal=minimal,
        data_source=data_source
    )
    price_df = loader.download()

    return price_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    START = '2020-02-02'
    END = '2020-04-02'
    ind_df, meta = get_ind_class(code_list=["ASM", "AAA"])
    p_df = get_price_from_ind_df(ind_df, START, END)

    logging.info(ind_df)
    logging.info(p_df)
