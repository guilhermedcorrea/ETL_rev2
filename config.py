from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import os
from pathlib import Path
from dotenv import load_dotenv
from urllib import parse



load_dotenv()



myboxuser_mssql = os.getenv('myboxmsqluser')
myboxpassword_mssql = os.getenv('myboxmsqlpassword')
myboxdatabase_mssql = os.getenv('myboxmsqldatabase')
myboxhost_mssql = os.getenv('myboxmmsqlhost')

user_mssql = os.getenv('serveruser')
password_mssql = os.getenv('serverpassword')
database_mssql = os.getenv('serverdatabase')
host_mssql = os.getenv('serverhost')



def mssql_get_conn():

    connection_url = URL.create(
            "mssql+pyodbc",
            username=f"{myboxuser_mssql}",
            password=f"{myboxpassword_mssql}",
            host=f"{myboxhost_mssql}",
            database=f"{myboxdatabase_mssql}",
            query={
                "driver": "ODBC Driver 17 for SQL Server",
                "autocommit": "True",
        },
        )
      
    engine = create_engine(connection_url).execution_options(
    isolation_level="AUTOCOMMIT", future=True,fast_executemany=True)
    return engine


def mssq_datawharehouse():

    connection_url = URL.create(
            "mssql+pyodbc",
            username=f"{user_mssql}",
            password=f"{password_mssql}",
            host=f"{host_mssql}",
            database=f"{database_mssql}",
            query={
                "driver": "ODBC Driver 17 for SQL Server",
                "autocommit": "True",
        }
        )
      
    engine = create_engine(connection_url).execution_options(
    isolation_level="AUTOCOMMIT", future=True,fast_executemany=True)
    return engine




def mssq_datawharehouselocal():

    connection_url = URL.create(
            "mssql+pyodbc",
            username=f"{localmsqluser}",
            password=f"{localmsqlpassword}",
            host=f"{localmmsqlhost}",
            database=f"{localmsqldatabase}",
            query={
                "driver": "ODBC Driver 17 for SQL Server",
                "autocommit": "True",
        }
        )
      
    engine = create_engine(connection_url).execution_options(
    isolation_level="AUTOCOMMIT", future=True,fast_executemany=True)
    return engine
