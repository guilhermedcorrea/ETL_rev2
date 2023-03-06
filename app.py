
from querys import retorna_venda_item, get_soma_ambiente
from sqlalchemy import text
from config import mssql_get_conn,mssq_datawharehouse
from itertools import chain
import pandas as pd
from sqlalchemy import insert
import numpy as np
from inserts import insert_dim_pedido, insert_fato_venda, insert_dim_tempo, insert_dim_clientes, insert_dim_produtos,insert_dim_endereco
from controllers import ajuste_strings_enderecos


def dim_pedidos():
    from querys import get_venda_item
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:

        pedidos = get_venda_item()

        exec_procedure = conn.execute(pedidos).all()
        dict_items = [row._asdict() for row in exec_procedure]
        pedidos_df = pd.DataFrame(dict_items)
        pedidos_df = pedidos_df.replace(np.nan, None)
        pedidos_df['nomeproduto'] = pedidos_df['nomeproduto'].apply(lambda  k: str(k).strip().capitalize())
        pedidos_df['dataCadastro'] = pedidos_df['dataCadastro'].apply(pd.to_datetime, infer_datetime_format=True)
        pedidos_df = pedidos_df.drop_duplicates()
        #print(pedidos_df)
        #print(pedidos_df.columns)
     
        pedidos_df_dict = pedidos_df.to_dict('records')
  
        insert_dim_pedido(*pedidos_df_dict)


def fato_venda():
    from querys import get_venda_pedidos

    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:

        pedidos = get_venda_pedidos()
        exec_procedure = conn.execute(pedidos).all()
        dict_items = [row._asdict() for row in exec_procedure]
        pedidos_df = pd.DataFrame(dict_items)
        pedidos_df = pedidos_df.replace(np.nan, None)
        
        pedidos_df.sort_values('dataCadastro',ascending=False)
        pedidos_df['dataCadastro'] = pedidos_df['dataCadastro'].apply(pd.to_datetime, infer_datetime_format=True)
        pedidos_df = pedidos_df.drop_duplicates()
        print(pedidos_df.columns)
      
        new_dict = pedidos_df.to_dict('records')
        insert_fato_venda(*new_dict)



def dim_tempo():
    from querys import get_venda_pedidos

    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:

        pedidos = get_venda_pedidos()
        exec_procedure = conn.execute(pedidos).all()
        dict_items = [row._asdict() for row in exec_procedure]
        pedidos_df = pd.DataFrame(dict_items)
        pedidos_df = pedidos_df.replace(np.nan, None)
        pedidos_df['dataCadastro'] = pedidos_df['dataCadastro'].apply(pd.to_datetime, infer_datetime_format=True)
        new_df = pedidos_df[['idvenda', 'idloja', 'NumeroContrato'
                    ,'dataCadastro', 'ano', 'mes', 'dia']]
        dict_item = new_df.to_dict('records')
        insert_dim_tempo(*dict_item)

        
def dim_cliente():
    from querys import get_clientes
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:

        pedidos = get_clientes()
        exec_procedure = conn.execute(pedidos).all()
        dict_items = [row._asdict() for row in exec_procedure]
        df_cliente = pd.DataFrame(dict_items)
        df_cliente = df_cliente.replace(np.nan, None)
        df_cliente['nomeCompletoRazaoSocial'] = df_cliente['nomeCompletoRazaoSocial'].apply(
            lambda k: str(k).strip().capitalize())
        
        df_cliente['cidade'] = df_cliente['cidade'].apply(lambda k: ajuste_strings_enderecos(k))
        df_cliente['logradouro'] = df_cliente['logradouro'].apply(lambda k: ajuste_strings_enderecos(k))
        df_cliente['bairro'] = df_cliente['bairro'].apply(lambda k: ajuste_strings_enderecos(k))
        df_cliente['enderecoId'] = df_cliente['enderecoId'].apply(lambda k: str(k).split(".0")[0].strip())
        df_cliente['enderecoId'] = df_cliente['enderecoId'].apply(lambda k: int(k) if k !="None" else k)
        new_dict = df_cliente.to_dict('records')
        insert_dim_clientes(*new_dict)



def dim_produtos():
    from querys import get_produtos
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:

        produtos = get_produtos()
        exec_procedure = conn.execute(produtos).all()
        dict_items = [row._asdict() for row in exec_procedure]
        df_produto = pd.DataFrame(dict_items)
        df_produto = df_produto.replace(np.nan, None)
        df_produto['nomeproduto'] = df_produto['nomeproduto'].apply(lambda k: str(k).strip().capitalize())
        new_dict = df_produto.to_dict('records')
        insert_dim_produtos(*new_dict)


def dim_endereco():
    #insert_dim_endereco
    from querys import get_clientes
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:

        pedidos = get_clientes()
        exec_procedure = conn.execute(pedidos).all()
        dict_items = [row._asdict() for row in exec_procedure]
        df_endereco = pd.DataFrame(dict_items)
        df_endereco = df_endereco.replace(np.nan, None)
        new_dict = df_endereco.to_dict('records')
        insert_dim_endereco(*new_dict)

dim_pedidos()