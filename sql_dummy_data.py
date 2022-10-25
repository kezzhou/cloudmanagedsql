#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#### Imports ####

import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from faker import Faker
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
        ## keep just the first 8 characters of the uuid
        'mrn': str(uuid.uuid4())[:8], 
        'first_name':fake.first_name(), 
        'last_name':fake.last_name(),
        'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('m', 'f')),
        'contact_mobile':fake.phone_number(),
        'contact_home':fake.phone_number(),
        'zip_code':fake.zipcode()
    } for x in range(50)] ## generate 50 patients

df_fake_patients = pd.DataFrame(fake_patients)
## drop duplicate mrns (in the case that there are) because mrns should be unique
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])


insertQuery = "INSERT INTO patients (mrn, first_name, last_name, dob, gender, contact_mobile, contact_home, zip_code) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)" ## %s indicates a dynamic value


for index, row in df_fake_patients.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'], row['dob'], row['gender'], row['contact_mobile'], row['contact_home'], row['zip_code']))
    print("inserted row: ", index)

df_azure = pd.read_sql_query("SELECT * FROM patients", db_azure)

df_azure ## we'll use df_azure to check that our populated tables went through. we can redefine it repeatedly as we proceed


## Medications

## id int auto_increment, ndc varchar(255) null unique, generic varchar(255) default null, active_ingredients varchar(255) default null

#### real ndc codes
ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1) ## take a sample of 1000 codes from the master list
## drop duplicates from ndc_codes_1k (ndc should also be unique)
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')

insertQuery = "INSERT INTO medications (ndc, generic, active_ingredients) VALUES (%s, %s, %s)"

medRowCount = 0
for index, row in ndc_codes_1k.iterrows():
    medRowCount += 1
    db_azure.execute(insertQuery, (row['PRODUCTNDC'], row['PROPRIETARYNAME'], row['NONPROPRIETARYNAME']))
    print("inserted row: ", index)
    ## stop once we have 100 rows
    if medRowCount == 100:
        break

df_azure = pd.read_sql_query("SELECT * FROM medications", db_azure)

df_azure


## Treatment Procedures

## id int auto_increment, cpt varchar(255) null unique, description varchar(255) default null

cpt_codes = pd.read_csv('https://gist.githubusercontent.com/lieldulev/439793dc3c5a6613b661c33d71fdd185/raw/25c3abcc5c24e640a0a5da1ee04198a824bf58fa/cpt4.csv')
cpt_codes_1k = cpt_codes.sample(n=1000, random_state=1)
## drop duplicates from cpt_codes_1k
cpt_codes_1k = cpt_codes_1k.drop_duplicates(subset=['com.medigy.persist.reference.type.clincial.CPT.code'], keep='first')

insertQuery = "INSERT INTO treatment_procedures (cpt, description) VALUES (%s, %s)"

medRowCount = 0 ## we can repurpose the iteration formula for each table besides the intermediary ones. if we wanted to, we could call this something else, such as treatRowNum
for index, row in cpt_codes_1k.iterrows():
    medRowCount += 1
    db_azure.execute(insertQuery, (row['com.medigy.persist.reference.type.clincial.CPT.code'], row['label']))
    print("inserted row: ", index)
    ## stop once we have 100 rows
    if medRowCount == 100:
        break

df_azure = pd.read_sql_query("SELECT * FROM treatment_procedures", db_azure)

df_azure


## Conditions

##  id int auto_increment, icd10 varchar(255) null unique, description varchar(255) default null

#### real icd10 codes
icd10codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10codes.columns)
icd10codesShort = icd10codes[['CodeWithSeparator', 'ShortDescription']]
icd10codesShort_1k = icd10codesShort.sample(n=1000)
## drop duplicates
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')

insertQuery = "INSERT INTO conditions (icd10, description) VALUES (%s, %s)"

startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    db_azure.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_azure: ", index)
    ## stop once we have 100 rows
    if startingRow == 100:
        break

df_azure = pd.read_sql_query("SELECT * FROM conditions", db_azure)

df_azure


## Social Determinants

##  id int auto_increment, loinc varchar(255) null unique, description varchar(255) default null

loinccodes = pd.read_csv('data/Loinc.csv')
list(loinccodes.columns)
loinccodesShort = loinccodes[['LOINC_NUM', 'COMPONENT']]
loinccodesShort_1k = loinccodesShort.sample(n=1000)
## drop duplicates
loinccodesShort_1k = loinccodesShort_1k.drop_duplicates(subset=['LOINC_NUM'], keep='first')

insertQuery = "INSERT INTO social_determinants (loinc, description) VALUES (%s, %s)"

startingRow = 0
for index, row in loinccodesShort_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    db_azure.execute(insertQuery, (row['LOINC_NUM'], row['COMPONENT']))
    print("inserted row db_azure: ", index)
    ## stop once we have 100 rows
    if startingRow == 100:
        break

df_azure = pd.read_sql_query("SELECT * FROM social_determinants", db_azure)

df_azure






## Intermediary Tables ##

## Patient Medications

## id int auto_increment, mrn varchar(255) default null, ndc varchar(255) default null

df_medications = pd.read_sql_query("SELECT ndc FROM medications", db_azure) ## we pull ndc from medications and mrn from patients table and push them into dataframes
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_azure)

## create a dataframe that is stacked and give each patient a random number of medications between 1 and 5
df_patient_medications = pd.DataFrame(columns=['mrn', 'ndc'])
for index, row in df_patients.iterrows():
    df_medications_sample = df_medications.sample(n=random.randint(1, 5)) ## patients will receive a random number of medications between 1 to 5 medications
    ## add the mrn to the df_medications_sample
    df_medications_sample['mrn'] = row['mrn']
    ## append the df_medications_sample to df_patient_medications
    df_patient_medications = df_patient_medications.append(df_medications_sample)

print(df_patient_medications.head(20)) ## check our work

## now lets assign drugs to patients randomly
insertQuery = "INSERT INTO patient_medications (mrn, ndc) VALUES (%s, %s)"

for index, row in df_patient_medications.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['ndc']))
    print("inserted row: ", index)

df_azure = pd.read_sql_query("SELECT * FROM patient_medications", db_azure)

df_azure


## Patient Conditions

## id int auto_increment, mrn varchar(255) default null, icd10 varchar(255) default null

df_conditions = pd.read_sql_query("SELECT icd10 FROM conditions", db_azure)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_azure)

## the process will be similar to medications

df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10'])
for index, row in df_patients.iterrows():
    df_conditions_sample = df_conditions.sample(n=random.randint(1, 5))
    df_conditions_sample['mrn'] = row['mrn']
    df_patient_conditions = df_patient_conditions.append(df_conditions_sample)

print(df_patient_conditions.head(20))

insertQuery = "INSERT INTO patient_conditions (mrn, icd10) VALUES (%s, %s)"

for index, row in df_patient_conditions.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['icd10']))
    print("inserted row: ", index)

df_azure = pd.read_sql_query("SELECT * FROM patient_conditions", db_azure)

df_azure



## Patient Treatment Procedures

## id int auto_increment,  mrn varchar(255) default null, cpt varchar(255) default null

df_treatment_procedures = pd.read_sql_query("SELECT cpt FROM treatment_procedures", db_azure)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_azure)

df_patient_treatment_procedures = pd.DataFrame(columns=['mrn', 'cpt'])
for index, row in df_patients.iterrows():
    df_treatment_procedures_sample = df_treatment_procedures.sample(n=random.randint(1, 5))
    df_treatment_procedures_sample['mrn'] = row['mrn']
    df_patient_treatment_procedures = df_patient_treatment_procedures.append(df_treatment_procedures_sample)

print(df_patient_treatment_procedures.head(20))

insertQuery = "INSERT INTO patient_treatment_procedures (mrn, cpt) VALUES (%s, %s)"

for index, row in df_patient_treatment_procedures.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['cpt']))
    print("inserted row: ", index)

df_azure = pd.read_sql_query("SELECT * FROM patient_treatment_procedures", db_azure)

df_azure


## Patient Social Determinants

## id int auto_increment, mrn varchar(255) default null, loinc varchar(255) default null

df_social_determinants = pd.read_sql_query("SELECT loinc FROM social_determinants", db_azure)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_azure)

df_patient_social_determinants = pd.DataFrame(columns=['mrn', 'loinc'])
for index, row in df_patients.iterrows():
    df_social_determinants_sample = df_social_determinants.sample(n=random.randint(1, 5))
    df_social_determinants_sample['mrn'] = row['mrn']
    df_patient_social_determinants = df_patient_social_determinants.append(df_social_determinants_sample)

print(df_patient_social_determinants.head(20))

insertQuery = "INSERT INTO patient_social_determinants (mrn, loinc) VALUES (%s, %s)"

for index, row in df_patient_social_determinants.iterrows():
    db_azure.execute(insertQuery, (row['mrn'], row['loinc']))
    print("inserted row: ", index)

df_azure = pd.read_sql_query("SELECT * FROM patient_social_determinants", db_azure)

df_azure


