
from sqlalchemy import insert
from querys import retorna_venda_item, get_soma_ambiente
from sqlalchemy import text
from config import mssql_get_conn,mssq_datawharehouse
from itertools import chain
import pandas as pd
from comercial_mybox_dw import DimPedido, FatoVenda



def check_fato_venda(*args, **kwargs):
    ...



def check_dim_pedido(*args, **kwargs):
    ...