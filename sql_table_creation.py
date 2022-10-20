#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#### Imports ####

import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os



#### Create connection with Azure for MySql Database with Dotenv ####

load_dotenv()

AZURE_MYSQL_HOSTNAME = os.getenv("AZURE_MYSQL_HOSTNAME")
AZURE_MYSQL_USER = os.getenv("AZURE_MYSQL_USERNAME")
AZURE_MYSQL_PASSWORD = os.getenv("AZURE_MYSQL_PASSWORD")
AZURE_MYSQL_DATABASE = os.getenv("AZURE_MYSQL_DATABASE")


connection_string_azure = f'mysql+pymysql://{AZURE_MYSQL_USER}:{AZURE_MYSQL_PASSWORD}@{AZURE_MYSQL_HOSTNAME}:3306/{AZURE_MYSQL_DATABASE}'
db_azure = create_engine(connection_string_azure)

#### note to self, need to ensure server_paremters => require_secure_transport is OFF in Azure 




#### Check Tables ####

print(db_azure.table_names()) ## at this point, there should be no tables until we run the mysql commands below, unless we are doing a rerun of the script




#### Drop Old Tables ####

## in the case that there are existing tables such as during a rerun of the script, this function will drop all tables indiscriminately
## there is no if function that would vet tables to drop.

def droppingFunction_all(dbList, db_source):
    for table in dbList:
        db_source.execute(f'drop table {table}')
        print(f'dropped table {table} succesfully!')
    else:
        print(f'task completed')

droppingFunction_all(db_azure.table_names(), db_azure) ## after defining the function we apply it to all table names found in our db connection


#### Creating tables within our selected database ####

## Patients

create_table_patients = """
create table if not exists patients (
    id int auto_increment,
    mrn varchar(255) null unique,
    first_name varchar(255) default null,
    last_name varchar(255) default null,
    dob varchar(255) default null,
    gender varchar(255) default null,
    contact_mobile varchar(255) default null,
    contact_home varchar(255) default null,
    zip_code varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""
## we use null unique to signify that there should be no repeating values
## this will be prevalent with unique identifiers, whether it's drug codes or social determinant codes
## we use default null to signify that the default value for an empty cell is null


## Medications

create_table_medications = """
create table if not exists medications (
    id int auto_increment,
    ndc varchar(255) null unique,
    brand_name varchar(255) default null,
    active_ingredients varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

## Treatments/Procedures

create_table_treatments_procedures = """
create table if not exists treatments_procedures (
    id int auto_increment,
    cpt varchar(255) null unique,
    treatments_procedures_name varchar(255) default null,
    description varchar(255) default null,
    PRIMARY KEY (id)
); 
"""

## Conditions

create_table_conditions = """
create table if not exists conditions (
    id int auto_increment,
    icd10 varchar(255) null unique,
    description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

## Social Determinants

create_table_social_determinants = """
create table if not exists social_determinants (
    id int auto_increment,
    loinc varchar(255) null unique,
    description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""



## Intermediary tables with Foreign Keys ##

## these tables will combine elements from other tables and be linked via foreign keys

## Patient Medications

create_table_patient_medications = """
create table if not exists patient_medications (
    id int auto_increment,
    mrn varchar(255) default null,
    ndc varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (ndc) REFERENCES medications(ndc) ON DELETE CASCADE
); 
"""

## Patients Conditions

create_table_patient_conditions = """
create table if not exists patient_conditions (
    id int auto_increment,
    mrn varchar(255) default null,
    icd10 varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (icd10) REFERENCES conditions(icd10) ON DELETE CASCADE
); 
"""

## our foreign keys cannot be edited in the tables in which they serve as foreign keys. They must be edited at the source of their linkage.


#### Execute our written commands with Python ####

db_azure.execute(create_table_patients)
db_azure.execute(create_table_medications)
db_azure.execute(create_table_treatments_procedures)
db_azure.execute(create_table_conditions)
db_azure.execute(create_table_social_determinants)
db_azure.execute(create_table_patient_medications)
db_azure.execute(create_table_patient_conditions)

print(db_azure.table_names()) ## we can check if our tables went through successfully