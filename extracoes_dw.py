

from sqlalchemy import Table
from sqlalchemy.orm import declarative_base
from sqlalchemy import Table, MetaData, Float, Integer,ForeignKey,DateTime, Boolean, String, Column
from datetime import datetime
from config import mssq_datawharehouse


engine = mssq_datawharehouse()
metadata = MetaData()
metadata_obj = MetaData(schema="comercial")



google_shopping = Table(
    "google_shopping",
metadata,
Column('cod_ambiente',Integer,primary_key=True),
Column('ref_produto',Integer),
Column('cod_barras',String),
Column('ref_categoria',Integer),
Column('ref_marca',Integer),
Column('preco_custo',Float),
Column('preco_venda',Float),
Column('nome_produto',String),
Column('preco_custo',Float),
Column('nome_concorrente',String),
Column('loja_venda',String),
Column('url_loja',String),
Column('preco_concorrente',Float),
Column('url_google',String),
Column('diferenca_preco',Float),
Column('canal_venda',String),
Column('data_atualizacao',DateTime)
,schema="comercial",implicit_returning=False,extend_existing=True)
