
from sqlalchemy import Table
from sqlalchemy.orm import declarative_base
from sqlalchemy import Table, MetaData, Float, Integer,ForeignKey,DateTime, Boolean, String, Column
from datetime import datetime
from config import mssql_get_conn


engine = mssql_get_conn()


metadata = MetaData()
metadata_obj = MetaData(schema="dbo")


#[myboxmarcenaria].[dbo].[VendaComunicacao]
VendaComunicacao = Table(
"VendaComunicacao",
metadata,
Column('id',Integer, primary_key=True),
Column('vendaId',Integer, ForeignKey('Venda.id')),
Column('fabricanteId',Integer, ForeignKey('Fabricante.id')),
Column('observacoes',String),
Column('dataCadastro',DateTime),
Column('statusId',Integer, ForeignKey('VendaStatus.id')),
Column('formaPagamento',String),
Column('faturamento',String),
Column('totalPedido',Float),
Column('dataLimite',DateTime),
Column('valorFinalPedido',Float),
Column('codigoRef',String),
Column('pedidoShowroom',Integer),
Column('transportadoraId',Integer),
Column('descritivoPlanejados',String),
Column('dataLimiteProducao',DateTime),
Column('dataEstimadaCliente',DateTime),
Column('valorFrete',Float),
Column('dataPrevisaoEntrega',DateTime),
Column('pontoAtencao',String),
schema="dbo",extend_existing=True)


 #[myboxmarcenaria].[dbo].[VendaItem]
VendaItem = Table(
    "VendaItem",
    metadata,
    Column('id',Integer, primary_key=True),
    Column('vendaId',Integer, ForeignKey('Venda.id')),
    Column('produtoId',Integer, ForeignKey('Produto.id')),
    Column('quantidade',Float),
    Column('observacoes',String),
    Column('valorUnitario',Float),
    Column('total',Float),
    Column('dataCadastro',DateTime),
    Column('observacoesPedido',String),
    Column('valorUnitarioCusto',Float),
    Column('produtoVariacaoId',Integer),
    Column('observacoesVariacao',String),
    Column('valorFrete',Float),
    schema="dbo",extend_existing=True)


#[myboxmarcenaria].[dbo].[VendaComunicacaoLog]
VendaComunicacaoLog = Table(
    "VendaComunicacaoLog",
    metadata,
    Column('id',Integer, primary_key=True),
    Column('dataCriacao',DateTime),
    Column('mensagem',String),
    Column('vendaComunicacaoId',String),
    Column('usuarioId',Integer),
    schema="dbo",extend_existing=True)


#[myboxmarcenaria].[dbo].[VendaStatus]
VendaStatus = Table(
    "VendaStatus",
    metadata,
    Column('id',Integer, primary_key=True),
    Column('nome',String),
    Column('prazoStatus',Integer),
    Column('statusDependenciaId',Integer),
    Column('ordem',Integer),
    Column('bitSite',Boolean),
    Column('icon',Integer),
    schema="dbo",extend_existing=True,implicit_returning=False)


Categorias = Table(
    "Categoria",
    metadata,
    Column("id", primary_key=True),
    Column("nome",String),
    Column("impostoMedio",Float),
    Column("margemMedia",Float),
    Column("habilitaSite",Boolean),
    Column("categoriaPaiId",Integer),
    Column("nivel",Integer),
)




#[myboxmarcenaria].[dbo].[Venda]
Venda = Table(
"Venda",
metadata,
Column('id',Integer, primary_key=True),
Column('pessoaId',Integer, ForeignKey('Pessoa.id')),
Column('statusId',Integer, ForeignKey('Status.id')),
Column('vendedorId',Integer),
Column('observacoes',String),
Column('totalProdutos',Float),
Column('totalProjeto',Float),
Column('totalFrete',Float),
Column('totalAdicional',Float),
Column('dataCadastro',DateTime),
Column('dataAlteracao',DateTime),
Column('dataEstimadaEntrega',DateTime),
Column('unidadeId',Integer, ForeignKey('Unidade.id')),
Column('inclusoFinanceiro',Integer),
Column('CodigoProjeto',Integer),
Column('bitVendaProgramada',Boolean),
Column('prazoVenda',Integer),
Column('totalDesconto',Float),
Column('numeracao',Integer),
Column('totalVenda',Float),
Column('excluido',DateTime),
Column('dataLimite',DateTime),
Column('bitSite',Boolean),
Column('dataEstimadaMontagem',DateTime),
Column('relacionamentoId',Integer),
Column('pedidoShowroom',Integer),
Column('origem',Integer),
Column('isOrcamentoWinn',Integer),
Column('dataInclusoFinanceiro',DateTime),
Column('motivoCancelamento',String),
Column('pedidoAvulsoAprovado',Integer),
Column('justificativaCompletandoContrato',String),
Column('pendenteExcluido',Boolean),
Column('dataPendenteExcluido',DateTime),
Column('usuarioExclusao',Boolean),
Column('statusExcluidoAprovacao',Boolean),
Column('assinado',Boolean),
Column('usuarioSolicitaExclusao',Integer),
schema="dbo",extend_existing=True)


#[myboxmarcenaria].[dbo].[Transportadora]
Transportadora = Table(
"Transportadora",
metadata,
Column('id',Integer, primary_key=True),
Column('ativo',Boolean),
Column('razaoSocial',String),
Column('nomeFantasia',String),
Column('email',String),
Column('emailAssistencia',String),
Column('emailFinanceiro',String),
Column('cnpj',String),
Column('ie',String),
Column('telefone',String),
Column('celular',String),
Column('cep',String),
Column('logradouro',String),
Column('numero',String),
Column('complemento',String),
Column('bairro',String),
Column('cidade',String),
Column('uf',String),
Column('dataCadastro',DateTime),
Column('dataAlteracao',DateTime),
Column('condicaoPgto',String),
Column('contatoAtendimento',String),
schema="dbo",extend_existing=True)


#[myboxmarcenaria].[dbo].[Cidades]
Cidades = Table(
"Cidades",
metadata,
Column('Codigo',Integer, primary_key=True),
Column('IBGE',String),
Column('Municipio',String),
Column('Uf',String),
Column('Populacao',String),
 schema="dbo",extend_existing=True)


#[myboxmarcenaria].[dbo].[NotaFiscal]
NotaFiscal = Table(
"NotaFiscal",
metadata,
Column('id',Integer, primary_key=True),
Column('vendaId',Integer, ForeignKey('Venda.id')),
Column('statusEmissor',String),
Column('bitErro',Boolean),
Column('bitEmitida',Boolean),
Column('idEmissor',Integer),
Column('formaPagamento',String),
Column('serie',Integer),
Column('numero',Integer),
Column('tipoOperacao',String),
Column('destino',String),
Column('tipoProposta',String),
Column('tipoVenda',String),
Column('compradorIndicadorTax',String),
Column('regimeTax',String),
Column('transporteNome',String),
Column('modalidadeFrete',String),
Column('informacoesAdicionais',String),
Column('valorTotal',Float),
Column('dataCadastro',DateTime),
Column('dataAtualizacao',DateTime),
Column('CodigoHttpRetorno',Integer),
Column('JsonBodyRequest',String),
Column('JsonBodyResponse',String),
schema="dbo",extend_existing=True)



Endereco = Table(
"Endereco",
metadata,
Column('id',Integer, primary_key=True),
Column('cep',String),
Column('cidade',String),
Column('complemento',String),
Column('logradouro',String),
Column('numero',String),
Column('uf',String),
Column('data',DateTime),
Column('bairro',String),
Column('latitude',String),
Column('longitude',String),
schema="dbo",extend_existing=True)



#[myboxm,arcenaria].[dbo].[NotaFiscal]
ProdutoPreco = Table(
"ProdutoPreco",
metadata,
Column('id',Integer, primary_key=True),
Column('produtoId',Integer, ForeignKey('Produto.id')),
Column('unidadeId',Integer, ForeignKey('Unidade.id')),
Column('precoVenda',Float),
Column('dataAlteracao',DateTime),
Column('custo',Float),
Column('produtoVariacaoIdd',Integer),
Column('descontoMaximoRecomendado',Float),
Column('icms',Float),
Column('st',Float),
Column('ipi',Float),
Column('royalties',Float),
Column('taxaFinanceira',Float),
Column('frete',Float),
Column('comissaoVendedor',Float),
Column('comissaoArquiteto',Float),
Column('precoCompletoSemComissao',Float),
schema="dbo",extend_existing=True)


#[myboxmarcenaria].[dbo].[Produto]
Produto = Table(
    "Produto",
    metadata,
    Column('id',Integer, primary_key=True),
    Column('nome',String),
    Column('excluido',Boolean),
    Column('estoqueMinimo',Integer),
    Column('saldoAtual',Float),
    Column('categoriaId',Integer, ForeignKey('Categoria.id')),
    Column('unidadeId',Integer, ForeignKey('Unidade.id')),
    Column('dataCadastro',DateTime),
    Column('produtoUnidadeId',Integer),
    Column('dataAlteracao',DateTime),
    Column('descricao',String),
    Column('marca',String),
    Column('caminhoFoto',String),
    Column('precoSugerido',Float),
    Column('precoCatalogoCusto',Float),
    Column('prazoEntregaMedio',Integer),
    Column('fabricanteId',Integer),
    Column('codigoFabricante',String),
    Column('CodigoBarras',String),
    Column('Largura',Integer),
    Column('Altura',Integer),
    Column('Profundidade',Integer),
    Column('Peso',Float),
    Column('NCM',String),
    Column('disponivel',Boolean),
    Column('variacao',Boolean),
    Column('habilitaSite',Boolean),
    Column('produtoPaiId',Integer),
    schema="dbo",extend_existing=True)

#[myboxmarcenaria].[dbo].[Pessoa]
Pessoa = Table(
    "Pessoa",
    metadata,
    Column('id',Integer, primary_key=True),
    Column('nomeCompletoRazaoSocial',String),
    Column('nomeFantasiaApelido',String),
    Column('ativo',Boolean),
    Column('excluido',Boolean),
    Column('email',String),
    Column('cpfCnpj',String),
    Column('enderecoId',Integer),
    Column('tipoId',Integer),
    Column('inscricaoMunicipal',String),
    Column('pessoafisica',Boolean),
    Column('responsavel',String),
    Column('responsavelTelefone',String),
    Column('celular',String),
    Column('telefone',String),
    Column('rgIe',String),
    Column('tipoSanguineo',),
    Column('dataCadastro',String),
    Column('dataNascimento',DateTime),
    Column('unidadeId',Integer),
    Column('responsavelCpf',String),
    Column('enderecoEntregaId',Integer),
    Column('indicadorIEDestinatario',Integer),
    Column('profissao',String),
    schema="dbo",extend_existing=True)


Unidade = Table(
    "Unidade",
    metadata,
    Column('id',Integer, primary_key=True),
    Column('nome',String),
    Column('celular',String),
    Column('telefone',String),
    Column('cnpj',String),
    Column('data',DateTime),
    Column('dataContrato',DateTime),
    Column('email',DateTime),
    Column('responsavel',DateTime),
    Column('tipoId',Integer),
    Column('enderecoId',Integer),
    Column('foto',String),
    Column('excluido',Boolean),
    Column('sempreExibir',Boolean),
    Column('telefoneExibir',Boolean),
    Column('codigoFabrica',Integer),
    Column('codigoProjeto',Integer),
    Column('numeracao',Integer),
    Column('responsavelNome',String),
    Column('codigoPixelFacebook',String),
    Column('whatsAppComunicacao',String),
    Column('latitude',String),
    Column('longitude',String),
    Column('codigoConversaoGoogle',Integer),
    Column('pessoaId',Integer),
    Column('endereçoEntregaId',Integer),
    Column('observacao',String),
    Column('ie',String),
    Column('dataAbertura',DateTime),
    Column('emailFaturamento',String),
    Column('instagram',String),
    Column('facebook',String),
    Column('regiao',String),
schema="dbo",extend_existing=True)


VendaComunicacaoStatus = Table(
    "VendaComunicacaoStatus",
    metadata,
    Column('id',Integer, primary_key=True),
    Column('nome',String),
    Column('prazoStatus',Integer),
    Column('ordemStatus',Integer),
    Column('bitSite',Boolean),
    schema="dbo",extend_existing=True)


ProdutoPreco = Table(
    "ProdutoPreco",
    metadata,
    Column('id',Integer, primary_key=True),
    Column('produtoId',Integer),
    Column('unidadeId',Integer),
    Column('precoVenda',Float),
    Column('dataAlteracao', DateTime),
    Column('custo',Float),
    Column('produtoVariacaoId',Integer),
    Column('descontoMaximoRecomendado',Float),
    Column('icms',Float),
    Column('st',Float),
    Column('ipi',Float),
    Column('royalties',Float),
    Column('taxaFinanceira',Float),
    Column('frete',Float),
    Column('comissaoVendedor',Float),
    Column('comissaoArquiteto',Float),
    Column('precoCompletoSemComissao',Float),
    schema="dbo",extend_existing=True)


VendaAmbiente = Table(
    "VendaAmbiente",
    metadata,
Column('id',Integer, primary_key=True),
Column('vendaId',Integer),
Column('ambiente',String),
Column('observacoes',String),
Column('valorAmbiente',Float),
Column('dataCadastro',DateTime),
Column('descritivo',String),
schema="dbo",extend_existing=True)


NotaFiscalAmbiente = Table(
    "NotaFiscalAmbiente",
    metadata,
Column('id',Integer, primary_key=True),
Column('notaFiscalId',Integer), 
Column('codigoAmbiente',String), 
Column('nome',String), 
Column('quantidade',Integer), 
Column('cfop',String), 
Column('codigoBarras',String), 
Column('valorIcms',Float), 
Column('valorPis',Float), 
Column('pisCst',String), 
Column('valorCofins',Float), 
Column('cofinsCst',String), 
Column('valorUnitario',Float), 
Column('valorTotal',Float), 
Column('ncm',String), 
Column('cest',String),
schema="dbo",extend_existing=True)


'''
CREATE TABLE [comercial].[dim_planejados_caracteristiacas](
	[cod_caracteristica] [int] IDENTITY(1,1) NOT NULL,
	[ambiente] [varchar](3000) NULL,
	[observacoes] [varchar](3000) NULL,
	[descritivo] [varchar](3000) NULL,
	[ref_planejado] [int] NULL,
	[data_cadastro] [datetime] NULL
) 
'''
