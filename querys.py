from sqlalchemy import text
from typing import Any


def get_unidades():
    call = (text("""
        SELECT distinct id,nome,enderecoId,endereçoEntregaId
        FROM [myboxmarcenaria].[dbo].[Unidade] """))
    return call
    
    
def get_soma_ambiente():
    call = (text("""
        
        select * ,SUM(V.totalAmbiente + v.totalItem) as totalVenda
        from (
        select distinct venda.id as vendaId
        ,unidadeId,
                    
                    case 
                        when ambiente.valorAmbiente is not null then 1
                        when ambiente.valorAmbiente is null then 0
                        else 0
                        end 'bitambientado'
        ,case
        when
        SUM(ambiente.valorAmbiente) OVER (PARTITION BY ambiente.vendaId) is null
        then
        0
        else
        SUM(ambiente.valorAmbiente) OVER (PARTITION BY ambiente.vendaId)
        end
        AS totalAmbiente
        ,case
        when
        SUM((vitem.valorUnitario * vitem.quantidade)) OVER (PARTITION BY vitem.vendaId) is null
        then
        0
        else
        SUM((vitem.valorUnitario * vitem.quantidade)) OVER (PARTITION BY vitem.vendaId)
        end
        AS totalItem
        from [dbo].[Venda] as venda
        left join [dbo].[VendaItem] as vitem on vitem.vendaId = venda.id
        left join [dbo].[VendaAmbiente] as ambiente on ambiente.vendaId = venda.id
        left join [dbo].Unidade as unidade on unidade.id = venda.unidadeId
        where unidade.id not in(1, 85, 89, 127)
        and unidade.excluido = 0
        and venda.statusId > 2
        and venda.numeracao <> 0
        and venda.excluido = 0
        ) V

        group by V.vendaId
        ,V.unidadeId
        ,V.totalAmbiente
        ,V.totalItem
                ,V.bitambientado"""))

    return call
    

def retorna_venda_item():

    call = (text("""
            SELECT DISTINCT CONVERT(INTEGER,VENDAS.id) idvenda, LOJA.id as idloja,CONVERT(INTEGER,VENDAS.numeracao) as NumeroContrato
            ,VENDAS.totalVenda,VENDAS.pessoaId,VENDAS.numeracao,VENDAS.totalProjeto,CONVERT(DATE,VENDAS.dataCadastro) as dataCadastro,year(VENDAS.dataCadastro) as ano,
            statusv.nome,VENDAS.pedidoShowroom,endereco.id as idendereco,statusv.id as idstatus,VENDAS.pedidoShowroom
            ,vitem.total,vitem.produtoId,
            CASE
            WHEN EXISTS(SELECT vamb.vendaId FROM [dbo].[VendaAmbiente]as vamb where vamb.vendaId = VENDAS.id) THEN 1
            WHEN NOT EXISTS (SELECT vamb.vendaId FROM [dbo].[VendaAmbiente]as vamb where vamb.vendaId = VENDAS.id) THEN 0
            ELSE 0
            END bitambient,

            CASE
            WHEN vitem.valorUnitario IS NULL THEN 0
            WHEN vitem.valorUnitario IS NOT NULL THEN vitem.valorUnitario
            ELSE 0
            END valorUnitario,

            CASE 
            WHEN vitem.quantidade IS NULL THEN 0
            WHEN vitem.quantidade IS NOT NULL THEN vitem.quantidade
            ELSE 0
            END quantidade,

            CASE
            WHEN vitem.valorUnitarioCusto IS NULL THEN 0
            WHEN vitem.valorUnitarioCusto IS NOT NULL THEN vitem.valorUnitarioCusto
            ELSE 0
            END valorUnitarioCusto,

            CASE
            WHEN vitem.valorUnitario IS NULL THEN 0
            WHEN vitem.valorUnitario IS NOT NULL THEN vitem.valorUnitario
            ELSE 0
            END valorUnitario

            
        from venda VENDAS
        left join [myboxmarcenaria].[dbo].[Unidade] AS LOJA ON LOJA.id = VENDAS.unidadeId
            left join [myboxmarcenaria].[dbo].[VendaStatus] as statuspv
            on statuspv.id = VENDAS.statusId
            left join [myboxmarcenaria].[dbo].[VendaComunicacaoStatus] as statusv
            ON statusv.id = VENDAS.statusId
            left join [myboxmarcenaria].[dbo].[Endereco] as endereco
            on endereco.id = LOJA.enderecoId
            left join [dbo].[VendaItem] as vitem
            on vitem.vendaId = VENDAS.id

        WHERE LOJA.id not in(1, 85, 89, 127) 
        and LOJA.excluido = 0
        and VENDAS.statusId > 2
        and VENDAS.excluido = 0
        and VENDAS.numeracao <> 0 and statusv.nome is not null"""))
    

    return call
                    


def get_clientes():
    call = (text("""
        SELECT DISTINCT pessoa.nomeCompletoRazaoSocial as nomecliente,pessoa.id,pessoa.cpfCnpj, pessoa.enderecoId,endereco.uf
        ,endereco.cep,endereco.cidade
        ,endereco.logradouro,endereco.complemento,endereco.uf,
        pessoa.pessoafisica  from [myboxmarcenaria].[dbo].[Pessoa] as pessoa
        inner join [myboxmarcenaria].[dbo].[Endereco] as endereco
        on endereco.id = pessoa.enderecoId
        WHERE pessoa.id in
        (SELECT DISTINCT vendas.pessoaId
                from [dbo].[Venda] vendas
                left join [myboxmarcenaria].[dbo].[Unidade] AS LOJA ON LOJA.id = VENDAS.unidadeId
                left join [myboxmarcenaria].[dbo].[VendaStatus] as statuspv
                on statuspv.id = VENDAS.statusId
                left join [myboxmarcenaria].[dbo].[VendaComunicacaoStatus] as statusv
                        ON statusv.id = VENDAS.statusId
                WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
                and LOJA.excluido = 0
                and VENDAS.statusId > 2
                and VENDAS.excluido = 0
                and VENDAS.numeracao <> 0)"""))
    return call
    
    

def get_categorias():
    call = (text("""
        SELECT  [id],[nome] ,[impostoMedio],[margemMedia],[habilitaSite] ,[categoriaPaiId]
        ,[nivel]  
        FROM [myboxmarcenaria].[dbo].[Categoria]"""))
    
    return call
    
    
    
    


def get_products_prices():
    call = (text("""SELECT CONVERT(INT,preco.id) AS idpreco,CONVERT(INT,preco.produtoId) AS produtoId
            ,CONVERT(int,preco.unidadeId) AS unidadeId,CONVERT(float,preco.precoVenda) AS precoVenda
            ,CONVERT(DATETIME,preco.dataAlteracao) AS dataAlteracao
            ,CONVERT(float,preco.custo) AS custo, CONVERT(float,preco.descontoMaximoRecomendado) AS descontoMaximoRecomendado
            ,CONVERT(float,preco.icms) AS icms,CONVERT(float,preco.st) st, CONVERT(float,preco.ipi) AS ipi,CONVERT(float,preco.royalties) AS royalties
            ,CONVERT(float,preco.taxaFinanceira) AS   taxaFinanceira    
            ,preco.frete ,preco.comissaoVendedor ,preco.comissaoArquiteto    
            ,preco.precoCompletoSemComissao, CONVERT(DATETIME, GETDATE()) as datacadastrodw, 
            ROUND(CONVERT(float,(CONVERT(float,preco.precoVenda)- (CONVERT(float,preco.custo)))),2) as lucro,
            ROUND(CONVERT(float,(CONVERT(float,preco.precoVenda)/ NULLIF(CONVERT(float,preco.custo),0)-1) * 100),2) as margem,
            CONVERT(float,CONVERT(float,preco.icms) +  CONVERT(float,preco.icms) + CONVERT(float,preco.st)
                            + CONVERT(float,preco.ipi) + CONVERT(float,preco.royalties)+ CONVERT(float,preco.taxaFinanceira)) AS outroscustos
                                
            FROM myboxmarcenaria.dbo.ProdutoPreco as preco
            where  preco.produtoId in (
            SELECT vitem.produtoId
            from venda VENDAS
            left join myboxmarcenaria.dbo.Unidade AS LOJA ON LOJA.id = VENDAS.unidadeId
            left join dbo.VendaItem as vitem
            on vitem.vendaId = VENDAS.id
            WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
            and LOJA.excluido = 0
            and VENDAS.statusId > 2
            and VENDAS.excluido = 0
            and VENDAS.numeracao <> 0 and LOJA.id = preco.unidadeId and preco.produtoId = vitem.produtoId)"""))

    return call

def get_status():
    call = (text("""SELECT [id],[nome],[prazoStatus],[ordemStatus],[bitSite] 
        FROM [myboxmarcenaria].[dbo].[VendaComunicacaoStatus]"""))
    return call
    
    

def get_fabricante():

    call = (text("""
            select distinct fabrica.ID,fabrica.Cep,fabrica.Cidade,fabrica.CNPJ,fabrica.NomeFabricante,fabrica.bitPlanejados
            ,fabrica.prazoEstimadoProducao,fabrica.prazoEstimadoFabrica,fabrica.UF
            , SUM(vitem.valorUnitario * vitem.quantidade) as totalvendidofabrica
            , SUM(vitem.valorUnitarioCusto * vitem.quantidade) as totalcusto, SUM(vitem.quantidade) as totalquantidade,
            SUM(COUNT(produto.id)) OVER (PARTITION BY fabrica.id) AS totalproduto

            from  [dbo].[VendaItem] vitem
            inner join [dbo].[Produto] as produto
            on produto.id = vitem.produtoId
            inner join [dbo].[Fabricante] as fabrica
            on fabrica.ID = produto.fabricanteId
            WHERE exists(

            SELECT DISTINCT fabrica.ID
            from [dbo].[Venda] vendas
            left join [myboxmarcenaria].[dbo].[Unidade] AS LOJA ON LOJA.id = VENDAS.unidadeId
            left join [myboxmarcenaria].[dbo].[VendaStatus] as statuspv on statuspv.id = VENDAS.statusId
            left join [myboxmarcenaria].[dbo].[VendaComunicacaoStatus] as statusv ON statusv.id = VENDAS.statusId
            left join [dbo].[VendaItem] vitem on vitem.vendaId = VENDAS.id
            left join [dbo].[Produto] as produto on produto.id = vitem.produtoId
            left join [dbo].[Fabricante] as fabrica on fabrica.ID = produto.fabricanteId

            WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
            and LOJA.excluido = 0
                            and VENDAS.statusId > 2
            and VENDAS.excluido = 0
            and VENDAS.numeracao <> 0 AND produto.id <> 0
            )
            GROUP BY fabrica.ID,fabrica.Cep,fabrica.Cidade,fabrica.CNPJ
            ,fabrica.prazoEstimadoProducao,fabrica.prazoEstimadoFabrica,fabrica.UF
            ,fabrica.slug,fabrica.NomeFabricante,fabrica.bitPlanejados


            """))

    return call


def get_enderecos_client():
    call = (text("""
                SELECT distinct endereco.[id],endereco.[cep],endereco.[cidade],endereco.[complemento],endereco.[logradouro]
                        ,endereco.[numero],endereco.[uf],endereco.[data] ,endereco.[bairro],endereco.[latitude]
                        ,endereco.[longitude],pessoa.id,pessoa.pessoafisica,pessoa.enderecoEntregaId
                        ,pessoa.nomeCompletoRazaoSocial as nomecliente,pessoa.id as idpessoa
                    FROM [myboxmarcenaria].[dbo].[Endereco] as endereco
                    inner join [dbo].[Pessoa] as pessoa
                    on pessoa.enderecoId = endereco.id"""))

    return call


def get_produtos_cadastros():
    call = (text("""
            SELECT distinct produto.[id],produto.[nome] ,produto.[categoriaId] ,produto.[unidadeId]
                ,produto.[marca],produto.[fabricanteId],produto.[CodigoBarras]
                ,categoria.nome as nomecategoria,produto.codigoFabricante
                FROM [myboxmarcenaria].[dbo].[Produto] as produto
                inner join [myboxmarcenaria].[dbo].[Categoria] as categoria
                on categoria.id = produto.categoriaId
                where produto.id in 
                (SELECT vitem.produtoId
                from venda VENDAS
                left join myboxmarcenaria.dbo.Unidade AS LOJA ON LOJA.id = VENDAS.unidadeId
                left join dbo.VendaItem as vitem on vitem.vendaId = VENDAS.id
                left join [dbo].[Pessoa] as pessoa on pessoa.id = VENDAS.pessoaId
                WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
                and LOJA.excluido = 0
                and VENDAS.statusId > 2
                and VENDAS.excluido = 0
                and VENDAS.numeracao <> 0)"""))

    return call


def get_frequencia_venda():
    call = (text("""
    
            select DISTINCT vitem.produtoId,vitem.vendaId,vitem.quantidade,vitem.valorFrete
            ,vitem.valorUnitarioCusto,vitem.valorUnitarioCusto
            ,fabricante.NomeFabricante,produto.marca,
            COUNT(vitem.produtoId) OVER(PARTITION BY vitem.vendaId) as totalitems,
            COUNT(produto.marca) OVER(PARTITION BY vitem.vendaId) as totalmarca,
            COUNT(vambiente.vendaId) OVER(PARTITION BY vitem.vendaId) as totalambiente
            from  [dbo].[VendaItem] as vitem
            inner join [myboxmarcenaria].[dbo].[Produto] as produto
            on produto.id = vitem.produtoId
            inner join [myboxmarcenaria].[dbo].[Fabricante] as fabricante
            on fabricante.id = produto.fabricanteId
            inner join [myboxmarcenaria].[dbo].[VendaAmbiente]as vambiente
            on vambiente.vendaId = vitem.vendaId

            where produto.id in (
            SELECT vitem.produtoId
            from venda VENDAS
            left join myboxmarcenaria.dbo.Unidade AS LOJA ON LOJA.id = VENDAS.unidadeId
            left join dbo.VendaItem as vitem on vitem.vendaId = VENDAS.id
            left join [dbo].[Pessoa] as pessoa on pessoa.id = VENDAS.pessoaId
            WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
            and LOJA.excluido = 0
            and VENDAS.statusId > 2
            and VENDAS.excluido = 0
            and VENDAS.numeracao <> 0) """))
    
    return call
                
    
   
def get_ambientados():
    call = (text("""select distinct 
            vitem.vendaId
            ,CONVERT(FLOAT,vitem.valorUnitario) AS valorUnitario
            ,CONVERT(FLOAT,vitem.valorUnitarioCusto) AS valorUnitarioCusto
            ,CONVERT(FLOAT,ambiente.valorAmbiente) AS valorAmbiente
            ,CONVERT(FLOAT,vitem.valorFrete) AS valorFrete
            ,CONVERT(datetime,vitem.dataCadastro) as dataCadastro
            ,CONVERT(INTEGER,venda.pessoaId) AS pessoaId
            ,CONVERT(INTEGER,venda.unidadeId) AS unidadeId
            ,CONVERT(INTEGER,venda.numeracao) AS numeracao
            ,venda.prazoVenda
            ,venda.pedidoShowroom
            ,CONVERT(INTEGER,venda.numeracao) AS numeracao
            ,ambiente.id as idambiente
            ,CONVERT(FLOAT,sum(CONVERT(FLOAT,(vitem.valorUnitario*vitem.quantidade)))) as total
            ,CONVERT(FLOAT,sum(ambiente.valorAmbiente)) AS somaambiente
            from [myboxmarcenaria].[dbo].[VendaItem] as vitem
            left join [myboxmarcenaria].[dbo].[VendaAmbiente] ambiente
            on ambiente.vendaId = vitem.vendaId
            left join [myboxmarcenaria].[dbo].[Venda] as venda
            on venda.id = vitem.vendaId
            where ambiente.[vendaId] in (
                SELECT VENDAS.id
                from venda VENDAS
                            left join myboxmarcenaria.dbo.Unidade AS LOJA ON LOJA.id = VENDAS.unidadeId
                            left join dbo.VendaItem as vitem on vitem.vendaId = VENDAS.id
                            left join [dbo].[Pessoa] as pessoa on pessoa.id = VENDAS.pessoaId
                            WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
                            and LOJA.excluido = 0
                            and VENDAS.statusId > 2
                            and VENDAS.excluido = 0
                            and VENDAS.numeracao <> 0)
                        GROUP BY vitem.vendaId,vitem.valorUnitario,vitem.valorUnitarioCusto, ambiente.valorAmbiente,
                        vitem.valorFrete,vitem.dataCadastro,venda.pessoaId,venda.unidadeId,venda.numeracao,
                        venda.prazoVenda,venda.pedidoShowroom,ambiente.id"""))
            
    return call


def get_notas():
    call = (text("""
    
        SELECT [id],[vendaId],[statusEmissor],[bitErro]
            ,[bitEmitida],[idEmissor] ,[formaPagamento] ,[serie],[numero]    
            ,[tipoOperacao],[destino] ,[tipoProposta],[tipoVenda] ,[compradorIndicadorTax]   
            ,[regimeTax],[transporteNome],[modalidadeFrete]     
            ,[informacoesAdicionais] ,[valorTotal] ,[dataCadastro]   
            ,[dataAtualizacao],[CodigoHttpRetorno],[JsonBodyRequest]       
            ,[JsonBodyResponse]     
            FROM [myboxmarcenaria].[dbo].[NotaFiscal] 
            
            where exists(
            SELECT vitem.vendaId
            from venda VENDAS
            left join myboxmarcenaria.dbo.Unidade AS LOJA ON LOJA.id = VENDAS.unidadeId
            left join dbo.VendaItem as vitem on vitem.vendaId = VENDAS.id
            left join [dbo].[Pessoa] as pessoa on pessoa.id = VENDAS.pessoaId
            WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
            and LOJA.excluido = 0
            and VENDAS.statusId > 2
            and VENDAS.excluido = 0
            and VENDAS.numeracao <> 0)"""))
    return call            
                
                
def get_nota_ambiente():
    notas = (text("""
        SELECT [id],[notaFiscalId],[codigoAmbiente],[nome],[quantidade],[cfop]
      ,[codigoBarras],[valorIcms],[valorPis],[pisCst],[valorCofins]
      ,[cofinsCst],[valorUnitario],[valorTotal]
      ,[ncm] ,[cest]
        FROM [myboxmarcenaria].[dbo].[NotaFiscalAmbiente]"""))
    
    return notas
    
    
def get_venda_pedidos():
    venda = (text("""SELECT distinct CONVERT(INTEGER,vendas.id) idvenda

			, LOJA.id as idloja,CONVERT(INTEGER,VENDAS.numeracao) as NumeroContrato
            ,vendas.totalVenda
			,vendas.pessoaId
			,vendas.numeracao
			,vendas.totalProjeto
			,CONVERT(DATE,VENDAS.dataCadastro) as dataCadastro
			,year(vendas.dataCadastro) as ano
			,month(vendas.dataCadastro) as mes
			,day(vendas.dataCadastro) as dia
            ,statusv.nome as statusnome
			,VENDAS.pedidoShowroom
			,endereco.id as idendereco
			,vendas.totalProjeto
			,vendas.totalProdutos
			,vendas.totalFrete
			,vendas.totalProdutos
			,vendas.totalDesconto
			,statusv.id as idstatus,
            CASE
            WHEN EXISTS(SELECT vamb.vendaId FROM [dbo].[VendaAmbiente]as vamb where vamb.vendaId = vendas.id) THEN 1
            WHEN NOT EXISTS (SELECT vamb.vendaId FROM [dbo].[VendaAmbiente]as vamb where vamb.vendaId = VENDAS.id) THEN 0
            ELSE 0
            END bitambient
            
        from venda as vendas
        inner join [myboxmarcenaria].[dbo].[Unidade] AS LOJA ON LOJA.id = vendas.unidadeId
            left join [myboxmarcenaria].[dbo].[VendaStatus] as statuspv
            on statuspv.id = VENDAS.statusId
            left join [myboxmarcenaria].[dbo].[VendaComunicacaoStatus] as statusv
            ON statusv.id = VENDAS.statusId
            left join [myboxmarcenaria].[dbo].[Endereco] as endereco
            on endereco.id = LOJA.enderecoId

        WHERE LOJA.id not in(1, 85, 89, 127) 
        and LOJA.excluido = 0
        and VENDAS.statusId > 2
        and VENDAS.excluido = 0
        """))
    return venda


def get_venda_item():
        
    vitem = (text("""select distinct 
            vendaitem.produtoId
            ,vendaitem.vendaId
            ,vendaitem.valorUnitario
            ,vendaitem.total
            ,venda.numeracao
            ,venda.pessoaId
            ,vendaitem.quantidade
            ,venda.pedidoShowroom
            ,venda.dataCadastro
            ,vendaitem.valorUnitarioCusto
            ,pessoa.enderecoEntregaId
            ,year(venda.dataCadastro) as ano
            ,month(venda.dataCadastro) as mes
            ,day(venda.dataCadastro) as dia
            ,pessoa.pessoafisica
            ,endereco.uf as ufcliente
            ,endereco.logradouro as logradourocliente
            ,endereco.cep as cepcliente
            ,endereco.cidade as cidadecliente
            ,endereco.id as idendereco
            ,vstatus.nome as nomestatus
            ,vstatus.id as idstatus
            ,produto.nome,produto.marca
            ,venda.totalVenda,venda.totalProjeto
            ,venda.totalProdutos
            ,venda.totalFrete
            ,venda.totalDesconto
            ,unidade.nome as nomeloja
            ,unidade.id as idunidade
            ,vstatus.nome as nomestatus
            ,vstatus.id as idstatus
            ,produto.categoriaId
            ,produto.CodigoBarras
            ,produto.codigoFabricante
            ,produto.nome as nomeproduto,
            CASE
            WHEN exists(select vambient.vendaId from [dbo].[VendaAmbiente] as  vambient where vambient.vendaId = venda.id) THEN 1
            WHEN not exists(select vambient.vendaId from [dbo].[VendaAmbiente] as  vambient where vambient.vendaId = venda.id) THEN 0
            ELSE 0
            END bitambiente

            from [dbo].[Venda] as venda
            left join [dbo].[VendaItem] as vendaitem on venda.id = vendaitem.vendaId
            left join [dbo].[Pessoa] as pessoa on pessoa.id = venda.pessoaId
            left join [dbo].[Endereco] as endereco on endereco.id = pessoa.enderecoId
            left join [dbo].[Produto] as produto on produto.id = vendaitem.produtoId
            left join [dbo].[VendaAmbiente] as vambiente on vambiente.vendaId = venda.id
            left join [dbo].[Unidade] as unidade on unidade.id = venda.unidadeId
            left join [dbo].[VendaStatus] as vstatus on vstatus.id = venda.statusId
        
            WHERE unidade.id not in(1, 85, 89, 127) 
                    and unidade.excluido = 0
                    and venda.statusId > 2
                    and venda.excluido = 0"""))
                    
    return vitem



def get_clientes():
    call = (text("""
        SELECT distinct pessoa.id as idpessoa,pessoa.nomeCompletoRazaoSocial,pessoa.ativo,pessoa.excluido
        ,pessoa.email,pessoa.cpfCnpj ,pessoa.enderecoId,pessoa.tipoId,endereco.uf
        ,endereco.cidade,endereco.logradouro,endereco.numero,endereco.bairro, endereco.cep, endereco.complemento
        FROM [myboxmarcenaria].[dbo].[Pessoa] as pessoa
        left join [dbo].[Endereco] as endereco on endereco.id = pessoa.enderecoId"""))
    
    return call
            
    

def get_produtos():
    call = (text("""
        select produto.id as idproduto
        ,produto.nome as nomeproduto, 
        produto.categoriaId
        ,produto.marca
        ,produto.CodigoBarras,
        produto.fabricanteId
        ,codigoFabricante from [dbo].[Produto] as produto
        left join [dbo].[VendaItem] as vitem on vitem.produtoId = produto.id
        where exists (select vitem.produtoId from [dbo].[VendaItem] as vitem where vitem.produtoId = produto.id)"""))
    
    return call
            
    
    
    