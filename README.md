# kaggle_credit_dataset_processing

This document is to process the following kaggle dataset, and to concat supplemental tables (POS_CASH_balance.csv, credit_card_balance.csv, previous_application.csv, installments_payments.csv, bureau_balance.csv and bureau.csv) into application_train.csv:
```
https://www.kaggle.com/c/home-credit-default-risk/data?select=application_train.csv
```

According to the dataset description, table bureau_balance.csv will firstly inner join bureau.csv on 'SK_ID_BUREAU' attribute, forming a combined bureau table. 
<!-- Secondly, the combined table will inner join application_train.csv on 'SK_ID_CURR'. -->

Then the following 4 tables, together with the above combined bureau table, will inner join application_train.csv on 'SK_ID_CURR' attribute:
POS_CASH_balance.csv
credit_card_balance.csv
previous_application.csv
installments_payments.csv


-------------------------
## Code scripts of processing the above operation are provided here


```
# import dependency
import pandas as pd
import os

# set directory
folder = '.../dataset/'

```
#### firstly, to prepare table 'bureau_balance', we need inner join 'bureau_balance' into 'bureau' on 'SK_ID_BUREAU' attribute
```
# set table name
idx = 'bureau_balance'

# set combined bureau table name to save
save_file_name = 'combined_bureau_table.csv'

# set which attribute to join on
attribute = 'SK_ID_BUREAU'

# read both supplemental table
bureau_balance=pd.read_csv(folder+idx+'.csv',dtype=object)
bureau=pd.read_csv(folder+'bureau.csv',dtype=object)
```
<!-- bureau=pd.DataFrame(bureau,columns=['SK_ID_CURR','SK_ID_BUREAU']) -->

By filling with empty string '', the csv file could save the empty value as '\<float> nan'(numpy.nan) type.
```
# create a new column as 'bureau_balance' in table 'bureau', and fill with empty string ''
bureau[idx] = ''
```
<!-- bureau.to_csv(save_file_name, mode='a',index=False) -->
<!-- # bureau_balance[bureau_balance['SK_ID_BUREAU']=='5714468'] # bureau_balance['SK_ID_BUREAU'].max() # len(bureau['SK_ID_BUREAU'].unique()) # len(bureau['SK_ID_CURR'].unique()) -->

```
# loop through 'bureau' entries
for i in bureau.index:
    # i as each entry index in bureau
    # extract new entries from 'bureau_balance' which share the same 'SK_ID_BUREAU' attribute in each 'bureau' entry, drop duplicated 'SK_ID_BUREAU' attribute
    selected_entries= (bureau_balance[bureau_balance[attribute] == bureau[attribute].loc[i]]).drop([attribute],axis=1)
    for j in selected_entries:
        # j as each attribute in columns, loop across columns
        if j == selected_entries.columns[0]:
            # convert the first selected 'bureau_balance' attribute into pandas Series, all values converted into string type
            str_list = selected_entries[j].map(str)
        else:
            # concat following selected 'bureau_balance' attributes, with '|' as the attribute separator, into pandas Series
            str_list += '|' + selected_entries[j].map(str)
    # use ';' as the entry separator, convert the obtained pandas Series into one long string
    str_list = ';'.join(str_list) if len(str_list) != 0 else ''
    # fill obtained string into the created new attribute in bureau table
    bureau.at[i,idx] = str_list
```
<!--     pd.DataFrame([ -->
<!--     bureau.at[i,'SK_ID_CURR'], bureau.at[i,'SK_ID_BUREAU'], str_list -->
<!--     ]).T.to_csv(save_file_name, mode='a',index=False,header=None) -->
```
# save the combined bureau table into 'combined_bureau_table.csv' with ',' as the attribute separator
bureau.to_csv(folder+save_file_name, mode='a',index=False,header=True, sep=',')
```

#### read 'application_train' table, set 'SK_ID_CURR' as key attribute for all prepared supplemental tables
```
# read target table 'application_train'
train=pd.read_csv(folder+'application_train.csv', dtype=object) 
# set which attribute to join on
attribute = 'SK_ID_CURR'
```
now, let's concat all prepared supplemental tables into 'application_train' table
```
# set supplemental table names
for idx in  ['previous_application', 'installments_payments', 'POS_CASH_balance', 'credit_card_balance', 'combined_bureau_table']:
    # read one supplemental table
    df=pd.read_csv(folder+'{}.csv'.format(idx), dtype=object)
    # create a new column with the supplemental table's name, and fill with empty string '' 
    train[idx] = ''

    for i in train.index:
        # i as each entry index in bureau 
        # extract new entries from supplemental table which share the same 'SK_ID_CURR' attribute in each train table entry, drop duplicated 'SK_ID_CURR' attribute
        selected_entries=(df[df[attribute] == train[attribute].loc[i]]).drop([attribute],axis=1)
        for j in selected_entries:
            # j as the each attribute in supplemental table columns, loop across columns
            if j == selected_entries.columns[0]:
                # convert the first selected supplemental attribute into pandas Series, all values converted into string type
                str_list = selected_entries[j].map(str)
            else:
                # concat following selected supplemental attributes, with '|' as the attribute separator, into pandas Series
                str_list += '|' +selected_entries[j].map(str)
        # use ';' as the entry separator, convert the obtained pandas Series into one long string
        str_list = ';'.join(str_list) if len(str_list) != 0 else ''
        # fill obtained string into the created new attribute in train table
        train.at[i,idx] = str_list

    save_file_name = 'data_{}.csv'.format(idx)
    # save supplemental table info into train table with ',' as the attribute separator
    train.to_csv(folder+save_file_name, mode='a',index=False,header=True, sep=',')
```
  

<!-- 
# re-read & inner join all tables
# train=pd.read_csv(folder+'train.csv', dtype=object) 
train=pd.read_csv(folder+'application_train.csv', dtype=object) 

loop::
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
