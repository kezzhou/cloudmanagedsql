#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#### Imports ####

import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from faker import Faker # https://faker.readthedocs.io/en/master/
import uuid
import random

## uuid and random are native packages, but faker must be installed locally via pip
## consult requirements.txt for more information


#### Create connection with Azure for MySql Database with Dotenv ####

load_dotenv()

AZURE_MYSQL_HOSTNAME = os.getenv("AZURE_MYSQL_HOSTNAME")
AZURE_MYSQL_USER = os.getenv("AZURE_MYSQL_USERNAME")
AZURE_MYSQL_PASSWORD = os.getenv("AZURE_MYSQL_PASSWORD")
AZURE_MYSQL_DATABASE = os.getenv("AZURE_MYSQL_DATABASE")


connection_string_azure = f'mysql+pymysql://{AZURE_MYSQL_USER}:{AZURE_MYSQL_PASSWORD}@{AZURE_MYSQL_HOSTNAME}:3306/{AZURE_MYSQL_DATABASE}'
db_azure = create_engine(connection_string_azure)



#### Check Tables ####

print(db_azure.table_names()) ## we should see five now



#### Inserting dummy data into newly created tables ####


## Patients

## id int auto_increment, mrn varchar(255), first_name varchar(255), last_name varchar(255), dob varchar(255), gender varchar(255), contact_mobile varchar(255), contact_home varchar(255), zip_code varchar(255) 

fake = Faker()

fake_patients = [
    {
        #keep just the first 8 characters of the uuid
        'mrn': str(uuid.uuid4())[:8], 
        'first_name':fake.first_name(), 
        'last_name':fake.last_name(),
        'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('m', 'f')),
        'contact_mobile':fake.phone_number(),
        'contact_home':fake.phone_number(),
        'zip_code':fake.zipcode()
    } for x in range(40)]

df_fake_patients = pd.DataFrame(fake_patients)
# drop duplicate mrn
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])


insertQuery = "INSERT INTO patients (mrn, first_name, last_name, dob, gender, contact_mobile, contact_home, zip_code) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"


for index, row in df_fake_patients.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'], row['dob'], row['gender'], row['contact_mobile'], row['contact_home'], row['zip_code']))
    print("inserted row: ", index)



## Medications

##    id int auto_increment, mrn varchar(255), brand_name varchar(255), active_ingredients varchar(255), ndc varchar(255)




## Treatments/Procedures

## id int auto_increment, treatments_procedures_name varchar(255), cpt varchar(255)



## Conditions

## id int auto_increment, mrn varchar(255), icd10 varchar(255), description varchar(255), treatments_procedures_id varchar(255)



## Social Determinants

## id int auto_increment, mrn varchar(255), loinc varchar(255), condition_id varchar(255)




db_azure.execute(insert_dummydata_patients)
db_azure.execute(insert_dummydata_medications)
db_azure.execute(insert_dummydata_treatments_procedures)
db_azure.execute(insert_dummydata_conditions)
db_azure.execute(insert_dummydata_social_determinants)






#### real icd10 codes
icd10codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10codes.columns)
icd10codesShort = icd10codes[['CodeWithSeparator', 'ShortDescription']]
icd10codesShort_1k = icd10codesShort.sample(n=1000)
# drop duplicates
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')



#### real ndc codes
ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1)
# drop duplicates from ndc_codes_1k
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')




########## INSERTING IN FAKE PATIENTS ##########
########## INSERTING IN FAKE PATIENTS ##########
########## INSERTING IN FAKE PATIENTS ##########
########## INSERTING IN FAKE PATIENTS ##########
########## INSERTING IN FAKE PATIENTS ##########
########## INSERTING IN FAKE PATIENTS ##########


#### Approach 1: pandas to_sql
#### Approach 1: pandas to_sql
#### Approach 1: pandas to_sql
#### Approach 1: pandas to_sql


# df_fake_patients.to_sql('production_patients', con=db_azure, if_exists='append', index=False)
# df_fake_patients.to_sql('production_patients', con=db_gcp, if_exists='append', index=False)

# # query db_azure to see if data is there
# df_azure = pd.read_sql_query("SELECT * FROM production_patients", db_azure)
# db_gcp = pd.read_sql_query("SELECT * FROM production_patients", db_gcp)

#### Approach 2: sqlalchemy with dynamic modification of values 
#### Approach 2: sqlalchemy with dynamic modification of values 
#### Approach 2: sqlalchemy with dynamic modification of values 
#### Approach 2: sqlalchemy with dynamic modification of values 


# # query dbs to see if data is there
# df_azure = pd.read_sql_query("SELECT * FROM production_patients", db_azure)
df_gcp = pd.read_sql_query("SELECT * FROM production_patients", db_gcp_2)









########## INSERTING IN FAKE CONDITIONS ##########

insertQuery = "INSERT INTO production_conditions (icd10_code, icd10_description) VALUES (%s, %s)"

startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    # db_azure.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_azure: ", index)
    db_gcp_2.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_gcp: ", index)
    ## stop once we have 100 rows
    if startingRow == 100:
        break

# query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM production_conditions", db_azure)
df_gcp = pd.read_sql_query("SELECT * FROM production_conditions", db_gcp)

# ###### the above way is inefficient, but it works. 
# ###### below is better if we have thousands/millions of rows to insert
# ##### for, for these big pushes, recommend using pandas to_sql, to do this, just need to make sure column names first match
# icd10codesShort_1k_mod = icd10codesShort_1k.rename(columns={'CodeWithSeparator': 'icd10_code', 'ShortDescription': 'icd10_description'})
# icd10codesShort_1k_mod.to_sql('production_conditions', con=db_azure, if_exists='replace', index=False)
# icd10codesShort_1k_mod.to_sql('production_conditions', con=db_gcp, if_exists='replace', index=False)













########## INSERTING IN FAKE MEDICATIONS ##########
########## INSERTING IN FAKE MEDICATIONS ##########
########## INSERTING IN FAKE MEDICATIONS ##########
########## INSERTING IN FAKE MEDICATIONS ##########

insertQuery = "INSERT INTO production_medications (med_ndc, med_human_name) VALUES (%s, %s)"

medRowCount = 0
for index, row in ndc_codes_1k.iterrows():
    medRowCount += 1
    # db_azure.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    db_gcp_2.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    print("inserted row: ", index)
    ## stop once we have 50 rows
    if medRowCount == 75:
        break

# ndc_codes_1k_moded = ndc_codes_1k.rename(columns={'PRODUCTNDC': 'med_ndc', 'NONPROPRIETARYNAME': 'med_human_name'})
# ndc_codes_1k_moded = ndc_codes_1k_moded.drop(columns=['PROPRIETARYNAME'])
# ## keep only first 100 characters for each med_human_name
# ndc_codes_1k_moded['med_human_name'] = ndc_codes_1k_moded['med_human_name'].str[:100]

# ndc_codes_1k_moded.to_sql('production_medications', con=db_azure, if_exists='replace', index=False)
# ndc_codes_1k_moded.to_sql('production_medications', con=db_gcp, if_exists='replace', index=False)

# query dbs to see if data is there
df_azure = pd.read_sql_query("SELECT * FROM production_medications", db_azure)
df_gcp = pd.read_sql_query("SELECT * FROM production_medications", db_gcp)






##### now lets create some fake patient_conditions 

# first, lets query production_conditions and production_patients to get the ids
df_conditions = pd.read_sql_query("SELECT icd10_code FROM production_conditions", db_gcp_2)
df_patients = pd.read_sql_query("SELECT mrn FROM production_patients", db_gcp_2)

# create a dataframe that is stacked and give each patient a random number of conditions between 1 and 5
df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10_code'])
# for each patient in df_patient_conditions, take a random number of conditions between 1 and 10 from df_conditions and palce it in df_patient_conditions
for index, row in df_patients.iterrows():
    # get a random number of conditions between 1 and 5
    # numConditions = random.randint(1, 5)
    # get a random sample of conditions from df_conditions
    df_conditions_sample = df_conditions.sample(n=random.randint(1, 5))
    # add the mrn to the df_conditions_sample
    df_conditions_sample['mrn'] = row['mrn']
    # append the df_conditions_sample to df_patient_conditions
    df_patient_conditions = df_patient_conditions.append(df_conditions_sample)

print(df_patient_conditions.head(20))

# now lets add a random condition to each patient
insertQuery = "INSERT INTO production_patient_conditions (mrn, icd10_code) VALUES (%s, %s)"

for index, row in df_patient_conditions.iterrows():
    db_gcp_2.execute(insertQuery, (row['mrn'], row['icd10_code']))
    print("inserted row: ", index)








##### now lets create some fake patient_medications

# first, lets query production_medications and production_patients to get the ids

df_medications = pd.read_sql_query("SELECT med_ndc FROM production_medications", db_gcp_2) 
df_patients = pd.read_sql_query("SELECT mrn FROM production_patients", db_gcp_2)

# create a dataframe that is stacked and give each patient a random number of medications between 1 and 5
df_patient_medications = pd.DataFrame(columns=['mrn', 'med_ndc'])
# for each patient in df_patient_medications, take a random number of medications between 1 and 10 from df_medications and palce it in df_patient_medications
for index, row in df_patients.iterrows():
    # get a random number of medications between 1 and 5
    numMedications = random.randint(1, 5)
    # get a random sample of medications from df_medications
    df_medications_sample = df_medications.sample(n=numMedications)
    # add the mrn to the df_medications_sample
    df_medications_sample['mrn'] = row['mrn']
    # append the df_medications_sample to df_patient_medications
    df_patient_medications = df_patient_medications.append(df_medications_sample)

print(df_patient_medications.head(10))

# now lets add a random medication to each patient
insertQuery = "INSERT INTO production_patient_medications (mrn, med_ndc) VALUES (%s, %s)"

for index, row in df_patient_medications.iterrows():
    db_gcp_2.execute(insertQuery, (row['mrn'], row['med_ndc']))
    print("inserted row: ", index)



### try and insert a new row with a random mrn and a random icd10_code
db_azure.execute(insertQuery, (random.randint(1, 1000000), random.choice(df_conditions['icd10_code'])))
## what happens and why? 