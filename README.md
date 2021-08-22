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


-----------------------
## Code script for making inner joined files

```python
# a list of files to inner join with application_train.csv
# note, according to the kaggle dataset website, bureau_balance.csv dose not connect to application_train.csv directly, so we don't process it here
file_to_inner_join = ['previous_application', 'installments_payments', 'POS_CASH_balance', 'credit_card_balance', 'bureau']
folder = '../dataset/'

base_file = pd.read_csv(folder+'{}.csv'.format('application_train'), dtype=object)

# firstly, make a new application_train.csv which only contains inner joined entries with all other files
for idx in file_to_inner_join:
    df=pd.read_csv(folder+'{}.csv'.format(idx), dtype=object)
    base_file=base_file[base_file['SK_ID_CURR'].isin(df['SK_ID_CURR'])]

# save this new application_train.csv
base_file.to_csv(folder+'inner_joined/inner_{}.csv'.format('application_train'), mode='a',index=False)

# secondly, based on this new application_train.csv, extract inner joined entries from all other files, and save 
for idx in file_to_inner_join:
    df=pd.read_csv(folder+'{}.csv'.format(idx), dtype=object)
    df=df[df['SK_ID_CURR'].isin(base_file['SK_ID_CURR'])]
    df.to_csv(folder+'inner_joined/inner_{}.csv'.format(idx), mode='a',index=False)
    if idx == 'burea':
        # now we handle bureau_balance.csv separately 
        df2=pd.read_csv(folder+'{}.csv'.format('bureau_balance'), dtype=object)
        # note, bureau_balance.csv connects to burea.csv on 'SK_ID_BUREAU' attribute
        df2=df2[df2['SK_ID_BUREAU'].isin(df['SK_ID_BUREAU'])]
        df2.to_csv(folder+'inner_joined/inner_{}.csv'.format('bureau_balance'), mode='a',index=False)
```


----------------------
## Code for describe dataset files


```python

import numpy as np

def get_stat(df_name, save_file, folder='dataset/', buffer=2):
    df = pd.read_csv(folder+'{}.csv'.format(df_name))
    # make a name list for one-hot convertable categorical columns 
    df_col = [* df.columns]
    # iterate through columns
    for col in df:
        # count categorical info
        df_describe = df[col].value_counts(dropna=False)
        # save the description into csv, including category info as index
        df_describe.to_csv(folder+'{}.csv'.format(save_file), mode='a',index=True)
        # obtain min/max/mean/std info only when the attributes are numerical values
        if (df_describe.index.dtype == float or df_describe.index.dtype == int) and df_describe.size > buffer: 
            (df[col].describe(include='all')).to_csv(folder+'{}.csv'.format(save_file), mode='a',index=True)
            # remove numerical columns names from list
            df_col.remove(col)
    return {df_name: df_col}



file_to_describe = ['previous_application', 'installments_payments', 'POS_CASH_balance', 'credit_card_balance', 'bureau', 'bureau_balance', 'application_train']

convert_col={}

for idx in file_to_describe:
    convert_col = {**get_stat(df_name = 'inner_joined/inner_{}'.format(idx), save_file = 'inner_stat/stat_{}'.format(idx), folder = '../dataset/'), **convert_col}
    
```    
------------------------
## Code for converting data into one-hot series


```python
import numpy as np

def to_one_hot(file_to_convert, save_file, folder='.../dataset/', folder2='.../dataset/'):
    df = pd.read_csv(folder+'{}.csv'.format(file_to_convert))
    for col in df:
        if col not in ['SK_ID_PREV', 'SK_ID_CURR', 'SK_ID_BUREAU']:
            df_describe = df[col].value_counts(dropna=False)
            if (df_describe.index.dtype == str) or (df_describe.index.dtype == 'O'):
                # create the rule for categorical string/Object attribute converting, and make a DF list
                mapping = dict((c, i) for i, c in enumerate(df_describe.index))
                df_cat = df_describe.index.astype(str)
                # category strings/Objects --> int 
                df[col] = [mapping[char] for char in df[col]]
                if df_describe.size == 2:
                    # for Female/Male similar category
                    df_cat = col + '_' + '/'.join(df_cat)
                    df = df.rename(columns = {col:df_cat}) # axis=1
                elif df_describe.size > 2:
                    df_cat = col + '_' + df_cat
                    # make one-hot series
                    one_hot=np.eye(df_describe.size)[df[col]].astype(int).astype(str)
                    # drop otiginal categorical string attribute
                    df = df.drop(col, axis=1)
                    # concate new one-hot encoding back to DF
                    df = pd.concat([df,pd.DataFrame(one_hot, columns=[*df_cat])], axis=1)
            elif df_describe.size > 2: 
                    df_col_as_float = df[col].astype('float')
                    # obtain statisics
                    df_describe = df_col_as_float.describe(include='all')
                    # normalise numerical attributes
                    df[col] = (df_col_as_float -  df_describe.loc['min']) / ( df_describe.loc['max'] - df_describe.loc['min'])
                    # rename the column, adding min~max value to its original name
                    df_cat = col + df_describe.loc['min'] + '~' + df_describe.loc['max']
                    df = df.rename(columns = {col:df_cat}) # axis=1
    df.to_csv(folder2+'{}.csv'.format(save_file), mode='a',index=False)
    
``` 


```python       
file_to_convert = ['previous_application', 'POS_CASH_balance', 'credit_card_balance', 'bureau', 'bureau_balance', 'installments_payments', 'application_train']
               
for idx in file_to_convert:
    to_one_hot('inner_'+idx, 'one_hot_'+idx, folder+'../dataset/inner_joined/', folder+'one_hot/')

```

<!-- redundant scripts for tips

            df_cat = df[col].astype('category').cat.categories.astype(str)
            df[col] = df[col].astype('category').cat.codes

convert_col = { 'previous_application': ['NAME_CONTRACT_TYPE', 'WEEKDAY_APPR_PROCESS_START', 'FLAG_LAST_APPL_PER_CONTRACT', 'NAME_CONTRACT_STATUS', 'NAME_PAYMENT_TYPE', 'CODE_REJECT_REASON', 'NAME_TYPE_SUITE', 'NAME_CLIENT_TYPE', 'NAME_GOODS_CATEGORY', 'NAME_PORTFOLIO', 'NAME_PRODUCT_TYPE', 'CHANNEL_TYPE', 'NAME_SELLER_INDUSTRY', 'NAME_YIELD_GROUP', 'PRODUCT_COMBINATION', 'NAME_CASH_LOAN_PURPOSE', 'HOUR_APPR_PROCESS_START'],
                'POS_CASH_balance':['NAME_CONTRACT_STATUS'],
                'credit_card_balance': ['CNT_DRAWINGS_OTHER_CURRENT', 'NAME_CONTRACT_STATUS'],
                'bureau': ['CREDIT_ACTIVE', 'CREDIT_CURRENCY', 'CNT_CREDIT_PROLONG', 'CREDIT_TYPE'],
                'bureau_balance': ['STATUS'],
                'train': [ 'NAME_CONTRACT_TYPE', 'CODE_GENDER', 'FLAG_OWN_CAR', 'FLAG_OWN_REALTY', 'CNT_CHILDREN', 'NAME_TYPE_SUITE', 'NAME_INCOME_TYPE', 'NAME_EDUCATION_TYPE', 'NAME_FAMILY_STATUS', 'NAME_HOUSING_TYPE', 'FLAG_MOBIL', 'OCCUPATION_TYPE', 'CNT_FAM_MEMBERS', 'REGION_RATING_CLIENT', 'REGION_RATING_CLIENT_W_CITY', 'WEEKDAY_APPR_PROCESS_START', 'HOUR_APPR_PROCESS_START', 'REG_REGION_NOT_WORK_REGION', 'REG_CITY_NOT_LIVE_CITY', 'LIVE_CITY_NOT_WORK_CITY', 'ORGANIZATION_TYPE', 'FONDKAPREMONT_MODE', 'HOUSETYPE_MODE', 'WALLSMATERIAL_MODE', 'EMERGENCYSTATE_MODE', 'DEF_30_CNT_SOCIAL_CIRCLE', 'DEF_60_CNT_SOCIAL_CIRCLE', 'AMT_REQ_CREDIT_BUREAU_HOUR', 'AMT_REQ_CREDIT_BUREAU_DAY', 'AMT_REQ_CREDIT_BUREAU_WEEK', 'AMT_REQ_CREDIT_BUREAU_MON', 'AMT_REQ_CREDIT_BUREAU_QRT', 'AMT_REQ_CREDIT_BUREAU_YEAR'],
               }

    df_col = [* df.columns]

     pd.DataFrame([ 
     bureau.at[i,'SK_ID_CURR'], bureau.at[i,'SK_ID_BUREAU'], str_list 
     ]).T.to_csv(save_file_name, mode='a', index=False, header=None) 
     

bureau.to_csv(save_file_name, mode='a',index=False) 
# bureau_balance[bureau_balance['SK_ID_BUREAU']=='5714468'] 
# bureau_balance['SK_ID_BUREAU'].max() 
# len(bureau['SK_ID_BUREAU'].unique()) 
# len(bureau['SK_ID_CURR'].unique())


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
