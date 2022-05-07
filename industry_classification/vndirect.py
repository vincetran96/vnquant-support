# Based on the industry classification API from VNDirect
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
import httpx
import logging
import urllib.parse
import pandas
from copy import deepcopy
from typing import List, Tuple
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
        code_list: List[str]=[],
        industry_codes: List[str]=[],
        higher_level_codes: List[str]=[],
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
    payload = deepcopy(BASE_PAYLOAD)
    payload_q_keys = deepcopy(PAYLOAD_Q_KEYS)
    payload_q_keys['industryLevel'] = DEFAULT_INDUSTRY_LEVEL
    payload_q_keys['codeList'] = ",".join([code for code in code_list])
    payload_q_keys['industryCode'] = ",".join([ic for ic in industry_codes])
    payload_q_keys['higherLevelCode'] = ",".join([hlc for hlc in higher_level_codes])
    payload_q_keys['englishName'] = english_name
    payload_q_keys['englishName'] = vietnamese_name
    payload_q_str = PAYLOAD_Q_JOIN_CHAR.join(
        [f"{key}:{value}" for key, value in payload_q_keys.items()]
    )
    payload['q'] = payload_q_str
    payload['size'] = result_size
    
    # Parse the payload dict using the payload safe chars
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

    # Process the JSON response
    # Then put industry data from it into a DataFrame
    # Metadata is everything else other than industry data
    resp_json = resp.json()
    ind_df = pandas.DataFrame(resp_json['data'])
    metadata_dict = {key: value for key, value in resp_json.items() if key != 'data'}

    return ind_df, metadata_dict


def get_full_ind_class():
    '''Gets full industry classification on VNDirect
    '''

    return get_ind_class()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info(get_ind_class(
        industry_codes=["8775", "8777", "8985"]
    ))
    logging.info(get_ind_class(
        code_list=["ASM", "AAA"]
    ))
    logging.info(get_ind_class())
