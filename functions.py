import calendar
import psycopg2 as pg
from urllib.parse import quote 
from datetime import datetime, timedelta
from dateutil import parser


def l_time():
    date = datetime.today()
    date = f'{date.hour}' + ':'+ f'{date.minute:02d}' + ':' + f'{date.second:02d}'
    return date

def days(date):
    date_string = date
    format = "%Y-%m-%d"
    day = datetime.strptime(date_string, format)
    day = calendar.day_name[day.weekday()]
    return day

def keep_date():
    date = datetime.today() - timedelta(31)
    date = date = f'{date.year}' + '-'+ f'{date.month:02d}' + '-' + f'{date.day:02d}'
    return date
    
def c_date (date1):
    date1 = parser.parse(date1)
    date2 = datetime.today()
    date2 = f'{date2.year}' + '-'+ f'{date2.month:02d}' + '-' + f'{date2.day:02d}'
    date2 = parser.parse(date2)
    diff = date2 - date1
    return diff.days

def nloop(start_date, ndays):
    date_for_c = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days = ndays)
    date_for_c = f'{date_for_c.year}' + '-'+ f'{date_for_c.month:02d}' + '-' + f'{date_for_c.day:02d}'
    return date_for_c

def del_dup(s_dest, t_dest, date):
    query = """
    delete from {s_dest}.{t_dest}
    where date = '{date}'
    and kpi in (select kpi from anomaly.kpi_list where status = 'include' and rate_status = 'N' and trend = 'F' and category = '{t_dest}')
    """.format(s_dest = s_dest, t_dest = t_dest, date = date)
    return query

def del_dup_t(s_dest, t_dest, date):
    query = """
    delete from {s_dest}.{t_dest}
    where date = '{date}'
    and kpi in (select kpi from anomaly.kpi_list where status = 'include' and rate_status = 'N' and trend = 'T' and category = '{t_dest}')
    """.format(s_dest = s_dest, t_dest = t_dest, date = date)
    return query

def del_dup_r(s_dest, t_dest, date):
    query = """
    delete from {s_dest}.{t_dest}
    where date = '{date}'and 
    kpi in (select kpi from anomaly.kpi_list where status = 'include' and rate_status = 'Y' and trend = 'F' and category = '{t_dest}')
    """.format(s_dest = s_dest, t_dest = t_dest, date = date)
    return query

def del_dup_r_t(s_dest, t_dest, date):
    query = """
    delete from {s_dest}.{t_dest}
    where date = '{date}'and 
    kpi in (select kpi from anomaly.kpi_list where status = 'include' and rate_status = 'Y' and trend = 'T' and category = '{t_dest}')
    """.format(s_dest = s_dest, t_dest = t_dest, date = date)
    return query

#CONNECTION
con_engine = ("postgresql+psycopg2://{user}:%s@{host}:{port}/{dbname}").format(
        host = '...', port = '...', dbname = '...', user = '...', password = '...')
conn_pg = pg.connect(host = '...', port = '...', dbname = '...', user = '...', password = '...')
