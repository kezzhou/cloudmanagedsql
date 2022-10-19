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

print(db_azure.table_names()) ## at this point, there should be no tables until we run the mysql commands below




#### Creating tables within our selected database ####

## Patients

create_table_patients = """
create table if not exists patients (
    id int auto_increment,
    mrn varchar(255),
    first_name varchar(255),
    last_name varchar(255),
    dob varchar(255),
    gender varchar(255),
    contact_mobile varchar(255),
    contact_home varchar(255),
    zip_code varchar(255),
    PRIMARY KEY (id) 
); 
"""

## Medications

create_table_medications = """
create table if not exists medications (
    id int auto_increment,
    mrn varchar(255),
    brand_name varchar(255),
    active_ingredients varchar(255),
    ndc varchar(255),
    PRIMARY KEY (id) 
); 
"""

## Treatments/Procedures

create_table_treatments_procedures = """
create table if not exists treatments_procedures (
    id int auto_increment,
    treatments_procedures_name varchar(255),
    cpt varchar(255),
    PRIMARY KEY (id)
); 
"""

## Conditions

create_table_conditions = """
create table if not exists conditions (
    id int auto_increment,
    mrn varchar(255),
    icd10 varchar(255),
    description varchar(255),
    treatments_procedures_id varchar(255),
    PRIMARY KEY (id) 
); 
"""

## Social Determinants

create_table_social_determinants = """
create table if not exists social_determinants (
    id int auto_increment,
    mrn varchar(255),
    loinc varchar(255),
    condition_id varchar(255),
    PRIMARY KEY (id) 
); 
"""

#### Execute our written commands with Python ####

db_azure.execute(create_table_patients)
db_azure.execute(create_table_medications)
db_azure.execute(create_table_treatments_procedures)
db_azure.execute(create_table_conditions)
db_azure.execute(create_table_social_determinants)