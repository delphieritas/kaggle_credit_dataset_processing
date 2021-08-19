# Kaggle 'Home Credit' Dataset Processing

This document is to process the following kaggle dataset, and to concat supplemental tables (POS_CASH_balance.csv, credit_card_balance.csv, previous_application.csv, installments_payments.csv, bureau_balance.csv and bureau.csv) into application_train.csv:
```
https://www.kaggle.com/c/home-credit-default-risk/data?select=application_train.csv
```

According to the dataset description, table bureau_balance.csv will firstly inner join bureau.csv on 'SK_ID_BUREAU' attribute, forming a combined bureau table. 

Then the following 4 tables, together with the above combined bureau table, will inner join application_train.csv on 'SK_ID_CURR' attribute:
POS_CASH_balance.csv
credit_card_balance.csv
previous_application.csv
installments_payments.csv

Each supplemental tables will eventually become relevant attributes in the final combined csv w.r.t. 'SK_ID_CURR'.

In consideration of keeping the potential connections among supplemental tables, we sort each supplemental table on their key attribute before padding them into final combined file.


-------------------------
## Code scripts of processing the above operation are provided here

import dependency and set dataset directory, pandas package is required for '1.3.1' version or higher
```python
import pandas as pd
import os

# set directory
folder = '.../dataset/'
```
define processing function
```python
def handler(base_table, supplemental_table, key_attribute, folder):
    '''
    base_table: a string of the base table name, e.g. 'application_train'
    supplemental_table: a list of supplemental csv file name strings, e.g. ['previous_application', 'installments_payments']
    key_attribute: a list of key_attribute strings, e.g. ['SK_ID_CURR', 'SK_ID_PREV']
    folder: a string of the dataset directory, e.g. '/tmp/user/Documents/Dataset/'
    '''

    # read base_table
    base_table = pd.read_csv(folder+'{}.csv'.format(base_table), dtype=object)
    # sort based on key_attribute
    base_table = base_table.sort_values(key_attribute,ignore_index=True)

    for idx in supplemental_table:
        # create a new column with supplemental_table name in base table, and fill with empty string ''
        # by filling with empty string '', the csv file could save the empty value as '\<float> nan'(numpy.nan) type.
        base_table[idx] = ''

        # read one supplemental_table as df
        df = pd.read_csv(folder+'{}.csv'.format(idx), dtype=object)
        # sort supplemental_table based on key_attribute
        df = df.sort_values(key_attribute,ignore_index=True)

        # loop through base_table entries
        for i in base_table.index:
            # i as each entry index in base_table
            # make mask from supplemental_table which share the same key_attribute in selected base_table entry
            mask = (df.loc[:,key_attribute] == base_table[key_attribute].iloc[i]).all(axis=1)
            if mask.any():
                # extract supplemental entries and drop duplicated key_attribute in  extracted supplemental_table
                selected_entries = df[mask].drop(key_attribute, axis=1)
                for j in selected_entries:
                    # j as each attribute in supplemental_table columns, loop across columns
                    if j == selected_entries.columns[0]:
                        # convert the first selected supplemental_table attribute into pandas Series, all values converted into string type
                        str_list = selected_entries[j].map(str)
                    else:
                        # concat following selected supplemental_table attributes, with '|' as the attribute separator, into pandas Series
                        str_list += '|' + selected_entries[j].map(str)
                # use ';' as the entry separator, convert the obtained pandas Series into one long string
                str_list = ';'.join(str_list) if len(str_list) != 0 else ''
                # fill the obtained string into the created new attribute in base_table
                base_table.at[i,idx] = str_list
        # inner join, remove redundant rows
        base_table = base_table.dropna(subset=[idx])
    return base_table
```
#### firstly, to prepare table 'bureau_balance', we need inner join 'bureau_balance' into 'bureau' on 'SK_ID_BUREAU' attribute
in the case of preparing 'bureau_balance', base_table refers to 'bureau', supplemental_table refers to 'bureau_balance'
```python
# set table names
supplemental_table = ['bureau_balance']
base_table = 'bureau'
# set which key_attribute to join on
key_attribute = ['SK_ID_BUREAU']

# set combined bureau table name to save
save_file = 'combine_{}'.format(base_table)
```
concat 'bureau_balance' table into 'bureau' table, and save
```python
combined_bureau = handler(base_table, supplemental_table, key_attribute, folder)
# save the combined base_table into 'combined_bureau.csv' with ',' as the attribute separator
combined_bureau.to_csv(folder+'{}.csv'.format(save_file), mode='a', index=False, header=True, sep=',')
```

#### read 'application_train' table, set 'SK_ID_CURR' as key attribute for inner join
in the case of concat 'application_train' with other tables, base_table refers to 'application_train', supplemental_table refers to the rest of tables, including the combined bureau table


note:

- in case the files are too large, it would be good to run each supplemental_table separately against 'application_train' table


```python
# set prepared supplemental_table names
supplemental_table = ['previous_application', 'installments_payments', 'POS_CASH_balance', 'credit_card_balance', supplemental_table[0]]
# set base table name
base_table = 'application_train'
# set which key_attribute to join on
key_attribute = ['SK_ID_CURR']
# set file name to save
save_file = 'combine_{}'.format(base_table)
```
now, let's concat all prepared supplemental tables into 'application_train' table, and save
```python
combined_train = handler(base_table, supplemental_table, key_attribute, folder)
combined_train.to_csv(folder+'{}.csv'.format(save_file), mode='a', index=False, header=True, sep=',')
```


----------------------
## Code for describe dataset files


```python

import numpy as np

def get_stat(df_name, save_file, folder='dataset/', buffer=32):
    df = pd.read_csv(folder+'{}.csv'.format(df_name))
    # make a name list for one-hot convertable categorical columns 
    df_col = [* df.columns]
    # iterate through columns
    for i in df:
        # count categorical info
        df_describe = df[i].value_counts(dropna=False)
        # save the description into csv, including category info as index
        df_describe.to_csv(folder+'{}.csv'.format(save_file), mode='a',index=True)
        # obtain min/max/mean/std info only when the attributes are numerical values
        if (type(df_describe.index[0])==np.float64 or type(df_describe.index[0])==np.int64) and df_describe.size>buffer: 
            (df[i].describe(include='all')).to_csv(folder+'{}.csv'.format(save_file), mode='a',index=True)
            # remove numerical columns names from list
            df_col.remove(i)
    return {df_name: df_col}



file_to_describe = ['previous_application', 'installments_payments', 'POS_CASH_balance', 'credit_card_balance', 'bureau', 'bureau_balance', 'application_train']

convert_col={}

for idx in file_to_describe:
    folder = 'dataset/inner_joined/'
    df_name = '{}'.format(idx)
    save_file = 'inner_stat/stat_{}'.format(idx)
    convert_col = {**get_stat(df_name, save_file, folder), **convert_col}
    
```    
------------------------
## Code for converting data into one-hot series


```python
import numpy as np
def to_one_hot(file_to_convert, save_file, folder='.../dataset/', folder2='.../dataset/', buffer=32, convert_col=[]):
    df = pd.read_csv(folder+'{}.csv'.format(file_to_convert), dtype=object)
    if len(convert_col)==0: convert_col = df.columns

    for col in convert_col:
        df_describe = df[col].value_counts(dropna=False)
        if df_describe.size <= buffer: 
            # create the rule for converting
            mapping = dict((c, i) for i, c in enumerate(df_describe.index))
            # convert categories into digits
            char_to_int = [mapping[char] for char in df[col]]
            # convert digits into one hot encoding
            one_hot=np.eye(len(mapping))[char_to_int].astype(int).astype(str)  # char_to_int = char_to_int.reshape(-1)
            # compact one hot numerical series into strings
            one_hot_str = [''.join(one_hot[x]) for x in range(len(one_hot))]
            df[col] = pd.DataFrame(one_hot_str)

    df.to_csv(folder2+'{}.csv'.format(save_file), mode='a',index=False)
``` 


```python       
buffer = 32
folder = '../dataset/inner_joined/'
file_to_convert = ['previous_application', 'installments_payments', 'POS_CASH_balance', 'credit_card_balance', 'bureau', 'bureau_balance', 'application_train']
folder2 = '../dataset/one_hot/'

for idx in file_to_convert:
    save_file = 'one_hot_{}'.format(idx)
    to_one_hot('inner_'+idx, save_file, folder, folder2, buffer, convert_col=convert_col[idx])

```





<!-- redundant scripts for tips -->
<!--
     pd.DataFrame([ 
     bureau.at[i,'SK_ID_CURR'], bureau.at[i,'SK_ID_BUREAU'], str_list 
     ]).T.to_csv(save_file_name, mode='a', index=False, header=None) -->
     
<!-- 
bureau.to_csv(save_file_name, mode='a',index=False) 
# bureau_balance[bureau_balance['SK_ID_BUREAU']=='5714468'] 
# bureau_balance['SK_ID_BUREAU'].max() 
# len(bureau['SK_ID_BUREAU'].unique()) 
# len(bureau['SK_ID_CURR'].unique()) -->

<!-- 
# re-read & inner join all tables
# train=pd.read_csv(folder+'train.csv', dtype=object) 
train=pd.read_csv(folder+'application_train.csv', dtype=object) 

# idx = os.getenv('TABLE_NAME')
for idx in ['previous_application','POS_CASH_balance','credit_card_balance','installments_payments']:
    df=pd.read_csv(folder+'data_{}.csv'.format(idx),dtype=object).dropna()
    # train=pd.concat([train,df], axis = 1).T.drop_duplicates().T #.columns
    train=pd.concat([train,df], axis = 1) #.columns
    # train.iloc[df.dropna().index] # .dropna() # .dropna(thresh=2) # .dropna(subset=['name', 'toy'])
    train=train.T.drop_duplicates().T

train=train.dropna(subset=['previous_application','POS_CASH_balance','credit_card_balance','installments_payments'])

# train.to_csv(folder+'train_dropna.csv', mode='a',index=False)
train.to_csv(folder+'train_dropna_index.csv', mode='a',index=True)
train_index=pd.read_csv(folder+'train_dropna_index.csv',dtype=object)
train_index=train_index.set_index('Unnamed: 0')

train_index=train_index.reset_index(drop=True)

# pd.concat([application_train, train], axis = 1).dropna(subset=['previous_application','POS_CASH_balance','credit_card_balance','installments_payments'])
# application_train.iloc[train[train['SK_ID_CURR']=='455993'].index]

if __name__ == "__main__":
    handler({'key1':1,'key2':2,'key3':3},None)
    print ('finished')
-->
