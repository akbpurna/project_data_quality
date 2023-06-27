from urllib.parse import quote 
from sqlalchemy import create_engine

#RECEIVE PARAMETER
param_host                      = '10.54.18.24'
param_port                      = '5432'
param_dbname                    = 'data_quality'
param_user                      = 'postgres' 
param_pw                        = 'P@ssw0rd*123'
param_schema_source             = 'data_master'
param_schema_destination        = 'completeness'
param_table_destination         = 'core' #cukup ganti ini saja, nanti dibuat dinamis berdasarkan pemanggilan
param_table_source_master       = 'data_quality_{}'.format(param_table_destination) #ganti nama tabelnya

# #CONNECT TO DB
con_engine = ("postgresql+psycopg2://{user}:%s@{host}:{port}/{dbname}" % quote('P@ssw0rd*123')).format(
        host = param_host, port = param_port, dbname = param_dbname, user = param_user, password = param_pw )
connection = create_engine(con_engine).connect()

n = 10

print("Start on : ", param_table_destination)

query1 = """delete from {a}.{b}
			--where 	date > '2022-08-17'
		 """.format(a=param_schema_destination, b=param_table_destination)

connection.execute(query1)

print("Data Deleted on : ", param_table_destination)

query2 ="""
insert into {a}.{b} (date, kpi, granularity, node, vendor, level, location, row_count, status)
select date, kpi, granularity, node, vendor, level, location, row_count, 'ok' as status
from(
	select
		*,
		RANK() over (partition by kpi, granularity, node, vendor, level, location order by date asc) ranking
	from {c}.{d}
	where 	date > '2022-08-17'
			and row_count is not null
			and row_count > 0

	) a
where ranking <= {n}
""".format(n=n, a=param_schema_destination, b=param_table_destination, c=param_schema_source, d=param_table_source_master)

connection.execute(query2)


print("Data has been inserted to : ", param_table_destination)
connection.close()