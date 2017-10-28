"""
Data collection script for mmb.moneycontrol.com
"""

import requests
from pandas import DataFrame
from pandas.io.json import json_normalize
try:
    data_set = DataFrame.from_csv('data_set.csv')
except Error as e:
    data_set = DataFrame()


def call_api(index):
    """
    @param :index: page number

    Returns JSON for `index`th page
    """
    print("calling ",index)
    response = requests.get('http://mmb.moneycontrol.com/index.php?q=home/ajax_call&section=get_messages&is_home_page=0&offset=&lmid=&isp=0&gmt=dm_lm&pgno='+ str(index))

    return response.json()


# pages to attempt
try_dwnload = 100

# list of already downloaded messages
if data_set.shape[0] < 1:
    already_dwnld = []
else:
    already_dwnld = list(data_set['msg_id'])
print('old -> ', data_set.shape)
# begin loop
for i in range(try_dwnload):
    data = call_api(i)
    for item in data:

        # ignore if msg_id already present
        if int(item['msg_id']) not in already_dwnld:
            if data_set.shape[0] < 1:
                data_set = json_normalize(item)
            else:
                data_set = data_set.append(json_normalize(item))
            # add this msg_id to already_dwnld list
            already_dwnld.append(int(item['msg_id']))

# post loop operations
print('new - > ', data_set.shape)
data_set.to_csv('data_set.csv')
