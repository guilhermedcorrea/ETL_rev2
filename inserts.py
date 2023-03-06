
from sqlalchemy import insert
from querys import retorna_venda_item, get_soma_ambiente
from sqlalchemy import text
from config import mssql_get_conn,mssq_datawharehouse
from itertools import chain
import pandas as pd
from comercial_mybox_dw import DimPedido, FatoVenda,dim_tempo,DimCliente, dim_produtos, DimEndereco


def insert_dim_pedido(*args, **kwargs):
    dwengine = mssq_datawharehouse()
    for arg in args:
        print(arg)
       
        with dwengine.connect() as conn:
                
                result = conn.execute(insert(DimPedido)
                                        ,[{"ref_contrato":arg["numeracao"],"ref_produto":arg["produtoId"],"ref_cliente":arg["pessoaId"],"valor_total":arg["totalVenda"],"frete_total":arg["totalFrete"]
                                           ,"ref_venda":arg["vendaId"],"ref_unidade":arg["idunidade"],"quantidade":arg["quantidade"],"valor_unitario":arg["valorUnitario"],"frete_unitario":arg["totalFrete"]
                                           ,"custounitario":arg["valorUnitarioCusto"],"bit_ambientado":arg["bitambiente"],"bit_showroom":arg["pedidoShowroom"],"ref_status":arg["idstatus"],"ref_endereco":arg["idendereco"]
                                           ,"pessoafisica":arg["pessoafisica"]
                                           ,"data_cadastro":arg["dataCadastro"],"mes_pedido":arg["mes"]}]) 
                                  
         
def insert_fato_venda(*args, **kwargs):
    dwengine = mssq_datawharehouse()
    for arg in args:
        print(arg)
    
        with dwengine.connect() as conn:
            result = conn.execute(insert(FatoVenda)
                                ,[{"cod_loja":arg["idloja"]
                                  ,"ref_venda":arg["idvenda"],"frete_total":arg["totalFrete"]
                                   ,"total_pedido":arg["totalVenda"],"ref_contrato":arg["NumeroContrato"],"nome_status":arg["statusnome"]
                                   ,"bitshowroom":arg["pedidoShowroom"]
                                   ,"bitambientado":arg["bitambient"],"data_cadastro":arg["dataCadastro"],"ref_cliente":arg["pessoaId"]
                                   ,"ref_unidade":arg["idloja"],"mes_pedido":arg["mes"]}])  
            


def insert_dim_tempo(*args, **kwargs):
    dwengine = mssq_datawharehouse()
    for arg in args:
        print(arg)
    
        with dwengine.connect() as conn:
            
            result = conn.execute(insert(dim_tempo)
                                ,[{"data_cadastro":arg["dataCadastro"]
                                  ,"mes_cadastro":arg["mes"],"dia_cadastro":arg["dia"],"ano_cadastro":arg["ano"]
                                  ,"ref_contrato":arg["NumeroContrato"],"ref_unidade":arg["idloja"],"ref_venda":arg["idvenda"]}])
            



def insert_dim_clientes(*args, **kwargs):
    dwengine = mssq_datawharehouse()
    for arg in args:
        print(arg)
    
        with dwengine.connect() as conn:
            try:
                result = conn.execute(insert(DimCliente)
                                    ,[{"nome_cliente":arg["nomeCompletoRazaoSocial"],"ref_cliente":arg["idpessoa"]
                                    ,"ref_endereco":arg["enderecoId"],"cidade":arg["cidade"]
                                    ,"uf":arg["uf"],"idenderecentrega":arg["enderecoId"],"bairro":arg["bairro"]}])
                
            except Exception as e:
                print(e)



def insert_dim_endereco(*args, **kwargs):
    dwengine = mssq_datawharehouse()
    for arg in args:
        print(arg)
  
        with dwengine.connect() as conn:
            try:
                result = conn.execute(insert(DimEndereco)
                                    ,[{"logradouro":arg["logradouro"],"complemento":arg["complemento"],"uf":arg["uf"],"cep":arg["cep"],"numero":arg["numero"]
                                       ,"cidade":arg["cidade"],"enderecoEntregaId":arg["enderecoId"],"ref_endereco":arg["enderecoId"]}])
                                     
                
            except Exception as e:
                print(e)
 

def insert_dim_produtos(*args, **kwargs):
    dwengine = mssq_datawharehouse()
    for arg in args:
        print(arg)
        with dwengine.connect() as conn:
                try:
                    result = conn.execute(insert(dim_produtos)
                                        ,[{"ref_produto":arg["idproduto"],"ref_categoria":arg["categoriaId"]
                                           ,"ref_fabricante":arg["fabricanteId"]
                                           ,"nome_produto":arg["nomeproduto"]
                                           ,"cod_barras":arg["CodigoBarras"],"marca":arg["marca"]}])
                    
                except Exception as e:
                    print(e)