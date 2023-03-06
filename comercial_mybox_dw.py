

from sqlalchemy import Table
from sqlalchemy.orm import declarative_base
from sqlalchemy import Table, MetaData, Float, Integer,ForeignKey,DateTime, Boolean, String, Column
from datetime import datetime
from config import mssq_datawharehouse


engine = mssq_datawharehouse()
metadata = MetaData()
metadata_obj = MetaData(schema="comercial")



jobs = Table(
"jobs",
metadata,
Column('idjobs',Integer, primary_key=True),
Column('data_execucao',DateTime),
Column('horas_execucao',Integer),
Column('intervalos_minutos',Integer),
Column('intervalos_horas',Integer),
Column('nome_job',String),
Column('tabela_referencia',String)
,schema="comercial",extend_existing=True)




#[comercial].[dim_pedido]
DimPedido = Table(
"dim_pedido",
metadata,
Column('cod_pedido',Integer, primary_key=True),
Column('ref_contrato',Integer),
Column('ref_produto',Integer),
Column('ref_cliente',Integer),
Column('valor_total',Float),
Column('frete_total',Float),
Column('ref_venda',Integer),
Column('ref_unidade',Integer),
Column('margem',Float),
Column('quantidade',Integer),
Column('valor_unitario',Float),
Column('frete_unitario',Float),
Column('custounitario',Float),
Column('bit_ambientado',Boolean),
Column('bit_showroom',Boolean),
Column('ref_status',Integer),
Column('ref_endereco',Integer),
Column('pessoafisica',Boolean),
Column('dataEstimadaEntrega',DateTime),
Column('data_montagem',DateTime),
Column('data_cadastro',DateTime),
Column('data_atualizacao',DateTime),
Column('adicionado_dw',DateTime),
Column('mes_pedido',Integer)
,schema="comercial",extend_existing=True,implicit_returning=False)



DimCidade = Table(
"dim_cidade",
metadata,
Column('cod_cidade',Integer,primary_key=True),
Column('nome_cidade',String),
Column('data_cadastro',DateTime),
Column('ref_endereco', String),
Column('uf', String)
,schema="comercial",extend_existing=True,implicit_returning=False)


DimCliente = Table(
"dim_cliente",
metadata,
Column('cod_cliente',Integer,primary_key=True),
Column('nome_cliente',String),
Column('ref_cliente',Integer),
Column('ref_endereco',Integer),
Column('bitPessoaFisica',Boolean),
Column('cidade',String),
Column('uf',String),
Column('idenderecentrega',Integer),
Column('bairro',String)
,schema="comercial",extend_existing=True)


DimCustos = Table(
"dim_custos",
metadata,
Column('cod_custos',Integer,primary_key=True),
Column('custo_total',Integer),
Column('frete',Float),
Column('royaltie',Float),
Column('valor_vendido',Float),
Column('desconto',Float),
Column('lucro',Float),
Column('margem',Float),
Column('valor_recomendado',Float),
Column('desconto_recomendado',Float),
Column('data_alteracao',DateTime),
Column('data_cadastro',DateTime),
Column('comissao_vendedor',Float),
Column('comissao_arquiteto',Float),
Column('icms',Float),
Column('st',Float),
Column('ipi', Float),
Column('custo', Float),
Column('outros_custos',Float),
Column('ref_produto', Integer),
Column('ref_loja',Integer)

,schema="comercial",extend_existing=True)
   


DimEndereco = Table(
"dim_endereco",
metadata,
Column('cod_endereco',Integer,primary_key=True),
Column('logradouro',String),
Column('complemento',String),
Column('uf',String),
Column('cep',String),
Column('numero',String),
Column('cidade',String),
Column('enderecoEntregaId',Integer),
Column('ref_endereco',Integer),
Column('data_atualizacao',DateTime),
schema="comercial",extend_existing=True,implicit_returning=False)



DimLoja = Table(
"dim_loja",
metadata,
Column('cod_loja',Integer,primary_key=True),
Column('nome_loja',String),
Column('ref_endereco',Integer),
Column('bitativo',Boolean),
Column('data_alteracao',DateTime),
Column('data_cadastro',DateTime),
Column('ref_loja', Integer)
,schema="comercial",implicit_returning=False,extend_existing=True)



DimMarca = Table(
"dim_marca",
metadata,
Column('cod_marca',Integer,primary_key=True),
Column('ticket_medido',Float),
Column('ref_fabricante',Integer),
Column('representatividade',Float),
Column('categoria_principal',Float),
Column('ref_marca',Integer),
Column('marca',String)
,schema="comercial",implicit_returning=False,extend_existing=True)




dim_notafiscal = Table(
"dim_notafiscal",
metadata,
Column('cod_nota',Integer,primary_key=True),
Column('ref_nota',Integer),
Column('ref_pedido',Integer),
Column('ref_cliente',Integer),
Column('data_emissao',DateTime),
Column('bitemissao',Boolean),
Column('bitcancelamento',Boolean),
Column('quantidade_itens',Integer),
Column('valor_nota',Float),
Column('dataatualizacaonf', DateTime),
Column('formapagamento',String),
Column('dataCadastro',DateTime)
,schema="comercial",implicit_returning=False,extend_existing=True)



dim_tempo = Table(
    "dim_tempo",
    metadata,
    Column('cod_tempo',Integer,primary_key=True),
    Column('data_cadastro',DateTime),
    Column('mes_cadastro',Integer),
    Column('dia_cadastro',Integer),
    Column('ano_cadastro',Integer),
    Column('data_atualizado',DateTime),
    Column('ref_contrato',Integer),
    Column('ref_unidade',Integer),
    Column('ref_cliente',Integer),
    Column('atualizadodw',DateTime),
    Column('ref_venda',Integer)
    ,schema="comercial",implicit_returning=False,extend_existing=True)


DimFabricante = Table(
    "dim_fabricante",
    metadata,
    Column('cod_fabricante', Integer, primary_key=True),
    Column('ref_fabricante',Integer),
    Column('nome_fabricante',String),
    Column('pedido_minimo',Float),
    Column('representatividade',Float),
    Column('data_ultimo_pedido',DateTime),
    Column('prazo_medio',Integer),
    Column('bitativo',Boolean),
    Column('planejados', Boolean),
    Column('total_produtos', Integer),
    Column('total_produtos_venda', Float),
    Column('total_custo_venda', Float),
    Column('data_atualizado', DateTime),
    Column('quantidade_skus', Integer)

,schema="comercial",implicit_returning=False,extend_existing=True)
    


DimStatus = Table(
"dim_status",
metadata,
Column('cod_status',Integer,primary_key=True),
Column('nome_status',String),
Column('data_cadastro',Integer),
Column('data_alteracao',Integer),
Column('ref_status',Integer)
,schema="comercial",implicit_returning=False,extend_existing=True)




DimPlanejadosCaracteristicas = Table(
"dim_planejados_caracteristiacas",
metadata,
Column('cod_caracteristica',Integer,primary_key=True),
Column('ambiente',String),
Column('observacoes',String),
Column('descritivo',String),
Column('ref_planejado',Integer),
Column('data_cadastro',DateTime)
,schema="comercial",extend_existing=True)

DimFrequencia = Table(
"dim_frequencia_venda",
metadata,
Column('cod_frequencia',Integer,primary_key=True),
Column('ref_produto',Integer),
Column('ref_venda',Integer),
Column('ref_loja',Integer),
Column('ref_cliente',Integer),
Column('ref_endereco',Integer),
Column('ref_fabricante',Integer),
Column('ref_categoria',Integer),
Column('ambientado',Boolean),
Column('showroom',Boolean),
Column('quantidade',Integer),
Column('valor_unitario',Float),
Column('valor_frete',Float),
Column('valor_desconto',Float),
Column('totalmarca', Integer),
Column('totalambiente',Integer)
,schema="comercial",extend_existing=True)


DimPlanejados = Table(
    "dim_planejados",
    metadata,
Column('cod_planjeado',Integer,primary_key=True),
Column('ref_pedido',Integer),
Column('ref_loja',Integer),
Column('ref_cliente',Integer),
Column('valor_venda',Float),
Column('data_cadastro',DateTime),
Column('ref_venda_ambiente',Integer)
,schema="comercial",extend_existing=True)


FatoVenda = Table(
"fato_venda",
metadata,
Column('cod_venda',Integer,primary_key=True),
Column('cod_status',Integer),
Column('cod_cliente',Integer),
Column('cod_loja',Integer),
Column('cod_endereco',Integer),
Column('cod_nota',Integer),
Column('ref_nota',Integer),
Column('ref_venda',Integer),
Column('custo_total',Float),
Column('frete_total',Float),
Column('lucro_total',Float),
Column('margem',Float),
Column('total_pedido',Float),
Column('ref_contrato',Integer),
Column('nome_status',String),
Column('quantidade_itens',Integer),
Column('bitshowroom',Boolean),
Column('bitentregue',Boolean),
Column('bitambientado',Boolean),
Column('data_cadastro',DateTime),
Column('ref_cliente',Integer),
Column('ref_unidade',Integer),
Column('cod_pedido',Integer),
Column('ultima_atualizacao',DateTime),
Column('cod_custos',Integer),
Column('adicionado_dw',DateTime),
Column('cod_tempo',Integer),
Column('mes_pedido',Integer)
,schema="comercial",implicit_returning=False,extend_existing=True)


dim_planejados_caracteristiacas = Table(
"dim_planejados_caracteristiacas",
metadata,
Column('cod_caracteristica',Integer,primary_key=True),
Column('ambiente',String),
Column('observacoes',String),
Column('descritivo',String),
Column('ref_planejado',Integer),
Column('data_cadastro',DateTime)
,schema="comercial",implicit_returning=False,extend_existing=True)


ambientados = Table(
    "dim_ambientados",
metadata,
Column('cod_ambiente',Integer,primary_key=True),
Column('ref_ambiente',Integer),
Column('ref_venda',Integer),
Column('ref_cliente',Integer),
Column('ref_loja',Integer),
Column('observacoes',String),
Column('valorAmbiente',Float),
Column('dataCadastro',DateTime),
Column('dataatualizado',DateTime),
Column('descritivo',String)
,schema="comercial",implicit_returning=False,extend_existing=True)


dim_produtos = Table(
    "dim_produtos",
    metadata,
Column('cod_produto',Integer, primary_key=True),
Column('ref_produto',Integer),
Column('ref_categoria',Integer),
Column('ref_fabricante',Integer),
Column('nome_produto',String),
Column('cod_barras',String),
Column('bit_ativo',Integer),
Column('marca',String)
,schema="comercial",implicit_returning=True,extend_existing=True)






dim_planejados_caracteristiacas = Table(
    "dim_planejados_caracteristiacas",
metadata,
Column('cod_caracteristica',Integer,primary_key=True),
Column('ambiente',String),
Column('observacoes',String),
Column('descritivo',String),
Column('ref_planejado',Integer),
Column('data_cadastro',DateTime)
,schema="comercial",implicit_returning=False,extend_existing=True)



dim_categorias = Table(
    "dim_categoria",
    metadata,
    Column('id',Integer,primary_key=True),
    Column('nome_categoria',String),
    Column('ref_categoria',Integer)
    ,schema="comercial",implicit_returning=False,extend_existing=True)





