import warnings
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine
from urllib.parse import quote 
from a_ok_funct  import  *

warnings.filterwarnings('ignore')

#RECEIVE PARAMETER
query, param_table_source_master = a_ok_r_nt('core', '2022-10-04')
print(query)
print(param_table_source_master)
# print(a_ok_r_nt('core', '2022-10-04'))