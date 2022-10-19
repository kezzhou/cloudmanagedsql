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



#### Check Tables ####

print(db_azure.table_names()) ## we should see five now



#### Inserting dummy data into newly created tables ####


## Patients

## id int auto_increment, mrn varchar(255), first_name varchar(255), last_name varchar(255), dob varchar(255), gender varchar(255), contact_mobile varchar(255), contact_home varchar(255), zip_code varchar(255) 

insert_dummydata_patients = """
insert into patients values 
(1, '418167217', 'johnny', 'jiang', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(2, '251184421', 'martin', 'noble', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'),
(3, '372363834', 'malachy', 'gallagher', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(4, '219361947', 'dakota', 'johnson', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(5, '392920687', 'sarah', 'singh', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(6, '901917262', 'ernest', 'antonino', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(7, '348185917', 'sheldon', 'barrett', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(8, '114828926', 'kevin', 'durant', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(9, '783017954', 'hants', 'williams', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(10, '645253474', 'rajiv', 'lajmi', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(11, '455815706', 'regina', 'chen', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(12, '162375516', 'angel', 'pagan', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(13, '712363603', 'william', 'gates', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(14, '402204622', 'jeffrey', 'bezos', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(15, '118405972', 'peter', 'parker', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(16, '760539145', 'michael', 'mecner', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(17, '544824386', 'melissa', 'mahoney', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(18, '986474475', 'sarah jessica', 'parker', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(19, '195780080', 'charles', 'white', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841'), 
(20, '802642069', 'john', 'doe', '10/01/98', 'm', '304-787-7265', '304-787-7265', '25841')
;
"""

db_azure.execute(insert_dummydata_patients)