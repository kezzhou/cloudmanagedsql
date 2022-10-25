# Cloud Managed SQL + ERDs + Dummy Data
HHA 504 // Week 7 // Assignment 6

## There are two main python components to this repository:

1. sql_table_creation.py: Python script that accesses Azure Database for MySql via dotenv credentials and creates the following tables within database patient_portal:
- patients
- medications
- treatment_procedures
- conditions
- social_determinants
- patient_medications
- patient_treatment_conditions
- patient_conditions
- patient_social_determinants

2. sql_dummy_data.py: Python script that uses imported packages to populate tables created by sql_table_creation.py with dummy data and randomized samples of publically available csvs.

## Description:

In this repo we continue our work with building a patient portal within a flask app. We create tables in an Azure Database for MySQL Server remotely through a create_engine connection with dotenv credentials. Then, we populate these empty tables with data found on the Internet pertaining to unique identifier codes. The sole exception is patients, with which we use Faker, an imported package designed to generate random persons data. Through our local terminals, we can check our work by accessing our Azure Database for MySQL Server and manually querying our databases. Finally, after installing MySqlWorkbench, we can reverse engineer entity relationship diagrams (ERD) to create a visual indicator of foreign key connections between tables.

## Resources:

[Faker Documentation](https://faker.readthedocs.io/en/master/)

[NDC Codes Github](https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv)

[ICD-10 Codes Github](https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv)

[CPT Codes Github](https://gist.githubusercontent.com/lieldulev/439793dc3c5a6613b661c33d71fdd185/raw/25c3abcc5c24e640a0a5da1ee04198a824bf58fa/cpt4.csv)

[LOINC Codes Official Site CSV Download](https://loinc.org/downloads/)

## Requirements:

- VS Code/Python Program of Choice
- MySqlWorkbench/popSQL/Equivalent
- Azure Database for MySQL Servers/GCP/Amazon/Equivalent
- Local Terminal/Cmd Prmpt/Equivalent
- MySqlClient

## Notes:

When deploying the Database for MySQL Server (SPECIFICALLY FOR AZURE), it's important to ensure that server_parameters > require_secure_transport is OFF. Otherwise local IPs will be barred from connecting to the server.

When creating connection strings, pymysql is the safest package to use for M1 processor Macbooks. Alternatives such as mysqldb may run compatibility errors.

Foreign key checks are on by default and will prevent blind dropping of tables. Turning it off before drop functions and back on afterwards is the way to go.

Users should avoid running entire scripts as it may overwhelm VS Code. It's suggested instead of run section by section.

As always, ensure that dotenv is included in gitignore as to not push sensitive log-in information onto Github.