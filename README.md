# kaggle_credit_dataset_processing

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

Each supplemental files will eventually become relevant attributes in the final combined csv w.r.t. 'SK_ID_CURR'.


-------------------------
## Code scripts of processing the above operation are provided here

import dependency and set dataset directory
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
    key_attribute: a string of the key attribute, e.g. 'SK_ID_BUREAU'
    folder: a string of the dataset directory, e.g. '/tmp/user/Documents/Dataset/'
    '''

    # read base_table
    base_table = pd.read_csv(folder+'{}.csv'.format(base_table), dtype=object)

    for idx in supplemental_table:
        # create a new column with supplemental_table name in base table, and fill with empty string ''
        # by filling with empty string '', the csv file could save the empty value as '\<float> nan'(numpy.nan) type.
        base_table[idx] = ''

        # read one supplemental_table as df
        df = pd.read_csv(folder+'{}.csv'.format(idx), dtype=object)

        # loop through base_table entries
        for i in base_table.index:
            # i as each entry index in base_table
            # extract entries from supplemental_table which share the same key_attribute in selected base_table entry, 
            # and drop duplicated key_attribute in  extracted supplemental_table
            selected_entries = (df[df[key_attribute] == base_table[key_attribute].loc[i]]).drop([key_attribute], axis=1)
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
    return base_table
```
#### firstly, to prepare table 'bureau_balance', we need inner join 'bureau_balance' into 'bureau' on 'SK_ID_BUREAU' attribute
in the case of preparing 'bureau_balance', base_table refers to 'bureau', supplemental_table refers to 'bureau_balance'
```python
# set table names
supplemental_table = ['bureau_balance']
base_table = 'bureau'
# set which key_attribute to join on
key_attribute = 'SK_ID_BUREAU'

# set combined bureau table name to save
save_file = 'combine_{}.csv'.format(base_table)
```
concat 'bureau_balance' table into 'bureau' table, and save
```python
combined_bureau = handler(base_table, supplemental_table, key_attribute, folder)
# save the combined base_table into 'combined_bureau.csv' with ',' as the attribute separator
combined_bureau.to_csv(folder+'{}.csv'.format(save_file), mode='a', index=False, header=True, sep=',')
```

#### read 'application_train' table, set 'SK_ID_CURR' as key attribute for inner join
in the case of concat 'application_train' with other tables, base_table refers to 'application_train', supplemental_table refers to the rest of tables, including the combined bureau table

```python
# set prepared supplemental_table names
supplemental_table = ['previous_application', 'installments_payments', 'POS_CASH_balance', 'credit_card_balance', supplemental_table[0]]
# set base table name
base_table = 'application_train'
# set which key_attribute to join on
key_attribute = 'SK_ID_CURR'
# set file name to save
save_file = 'combine_{}.csv'.format(base_table)
```
now, let's concat all prepared supplemental tables into 'application_train' table, and save
```python
combined_train = handler(base_table, supplemental_table, key_attribute, folder)
combined_train.to_csv(folder+'{}.csv'.format(save_file), mode='a', index=False, header=True, sep=',')
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
