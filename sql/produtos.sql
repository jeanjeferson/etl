SELECT
    RTRIM(a.colecoes) AS Cod_Modelo,
    CASE
        WHEN RTRIM(e.descs) LIKE '%COLECAO %' THEN REPLACE(RTRIM(e.descs), 'COLECAO ', '')
        WHEN RTRIM(e.descs) LIKE '%COLEÇÃO %' THEN REPLACE(RTRIM(e.descs), 'COLEÇÃO ', '')
        ELSE RTRIM(e.descs)
    END AS Colecao,
    RTRIM(a.cgrus) AS Cod_Gr,
    RTRIM(f.dgrus) AS Desc_Gr,
    RTRIM(a.sgrus) AS Cod_Subgr,
    RTRIM(g.descricaos) AS Desc_Subgr,
    RTRIM(a.cpros) AS Cod_Prod,
    RTRIM(a.dpros) AS Desc_Produto,
    CAST(a.markupa AS DECIMAL(18,4)) AS Markup,
    RTRIM(a.moecusfs) AS Moeda,
    CAST(h.valos AS DECIMAL(18,4)) AS Cotacao,
    CAST(a.pcuss AS DECIMAL(18,4)) AS Pr_Custo,
    MAX(CAST(a.pcuss * COALESCE(h.valos, 1) AS DECIMAL(18,4))) AS Custo_Real,
    CAST(a.pvens AS DECIMAL(18,4)) AS Pr_Venda,
    MAX(CAST(a.pvens * COALESCE(h.valos, 1) AS DECIMAL(18,4))) AS Pr_Venda_Real,
    RTRIM(a.mercs) AS Grande_Grupo,
    RTRIM(b.descs) AS Desc_Grande_Grupo,
    RTRIM(a.metals) AS Metal,
    RTRIM(a.codcors) AS Cor,
    RTRIM(c.descs) AS Desc_Cor,
    RTRIM(a.codscols) AS Sub_Nivel,
    RTRIM(d.descs) AS Desc_Sub_Nivel,
    RTRIM(a.cftios) AS Tab_Pr,
    RTRIM(a.CODFINP ) AS Cod_Finalidade,
    RTRIM(fin.descs ) AS Desc_Finalidade,
    CASE
        WHEN a.encoms = 1 THEN 'Sim'
        ELSE 'Nao'
    END AS [Encomendavel],
    CASE
        WHEN a.situas = 1 THEN 'Ativo'
        ELSE 'Inativo'
    END AS Status,
    CASE
    WHEN MAX(pv.Cod_Prod) IS NOT NULL THEN 'Sim'
        ELSE 'Nao'
    END AS Vendeu,
    CAST(a.dtincs AS DATE) AS Data_Inclusao,
    CAST(a.pesoms AS DECIMAL(18,4)) AS Peso,
    MIN(CAST(ISNULL(i.qmins, 0) AS DECIMAL(18,4))) AS Estoque_Minimo,
    MAX(CAST(i.qideal AS DECIMAL(18,4))) AS Estoque_Ideal,
    RTRIM(a.cclass) AS Classificacao,
    RTRIM(a.linhas) AS Linha
FROM sljpro a
    LEFT JOIN sljcor c ON a.codcors = c.cods
    LEFT JOIN sljgru f ON a.cgrus = f.cgrus
    LEFT JOIN sljsgru g ON a.cgrus + a.sgrus = g.cgrucods
    LEFT JOIN sljcol e ON a.colecoes = e.colecoes
    LEFT JOIN sljscol d ON a.codscols = d.codigos
    LEFT JOIN sljpremp i ON a.cpros = i.cpros
    LEFT JOIN sljcfinp fin ON a.codfinp = fin.cods
    LEFT JOIN SLJGGRP B ON B.codigos = a.mercs
    LEFT JOIN (
                SELECT a.valos, a.cmoes
    FROM sljcot a
        JOIN (
            SELECT MAX(datas) datas, cmoes
            FROM sljcot
            GROUP BY cmoes
                    ) b ON a.cmoes = b.cmoes AND a.datas = b.datas
                ) h ON RTRIM(a.moecusfs) = h.cmoes
    LEFT JOIN (
        SELECT DISTINCT
        RTRIM(vendas.cpros) AS Cod_Prod,
        CAST(SUM(vendas.qtds) AS BIGINT) AS Qtd,
        CAST(SUM(vendas.totas) AS DECIMAL(10,3)) AS Total_Liq
    FROM sljgdmi AS vendas
        LEFT JOIN sljpro prod WITH(NOLOCK) ON RTRIM(vendas.cpros) = RTRIM(prod.cpros)
    WHERE vendas.ggrus IN (
                    SELECT DISTINCT A.ggrus
        FROM SLJGDMI A JOIN SLJGGRP B ON A.ggrus = B.codigos AND B.relgers <> 2 )
        AND RTRIM(prod.mercs) IN ('PA', 'PRA')
    GROUP BY RTRIM(vendas.cpros)
            ) pv ON RTRIM(a.cpros) = pv.Cod_Prod
    WHERE B.relgers <> 2
    -- AND RTRIM(a.cpros) = 'JO23468'
    GROUP BY 
                a.colecoes,
                e.descs,
                a.cgrus,
                f.dgrus,
                a.sgrus,
                g.descricaos,
                a.cpros,
                a.dpros,
                a.markupa,
                a.moecusfs,
                a.pcuss,
                h.valos,
                a.pvens,
                a.mercs,
                b.descs,
                a.metals,
                a.codcors,
                c.descs,
                a.codscols,
                d.descs,
                a.cftios,
                a.CODFINP,
                fin.descs,
                a.encoms,
                a.situas,
                a.dtincs,
                a.pesoms,
                a.cclass,
                a.linhas




-- WHERE a.mercs IN (
--                 SELECT DISTINCT A.ggrus
-- FROM SLJGDMI A
--     JOIN SLJGGRP B
--     ON A.ggrus = B.codigos AND B.relgers <> 2
--             )


-- SELECT RTRIM(a.mercs ) AS Grande_Grupo FROM sljpro a
-- LEFT JOIN SLJGGRP B ON B.codigos = a.mercs
-- WHERE B.relgers <> 2
-- GROUP BY RTRIM(a.mercs)


-- SELECT a.colecoes FROM sljpro a

-- SELECT * FROM sljevent