import warnings
from sqlalchemy import create_engine
from functions import  con_engine, keep_date
warnings.filterwarnings('ignore')

connection = create_engine(con_engine).connect()
for c in ('availability', 'capacity', 'core', 'ran', 'revenue', 'transport', 'tutela', 'vas'):
    query = """ delete from anomaly.h_{category} where date <= {date}""".format(category = c, date=keep_date())
    connection.execute(query)
connection.close()

