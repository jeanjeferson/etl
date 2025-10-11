SELECT
    RTRIM(vendas.emps) AS Empresa,
    RTRIM(empresa.class) AS Classificacao_Emp,
    RTRIM(vendas.dopes) AS Operacao,
    RTRIM(vendas.tipoops) AS Tipo_operacao,
    CAST(vendas.numes AS BIGINT) AS No_Oper,
    CAST(vendas.datas AS DATE) AS Data,
    RTRIM(vendas.iclis) AS Cod_Cliente,
    RTRIM(vendas.rclis) AS Nome_Cliente,
    RTRIM(vendas.vends) AS Cod_Vend,
    RTRIM(consultora.RCLIS) AS Consultora,
    RTRIM(vendas.ggrus) AS Grande_Grupo,
    RTRIM(vendas.cpros) AS Cod_Prod,
    RTRIM(vendas.codbarras) AS Cod_Barras,
    RTRIM(vendas.codtams) AS Tamanho,
    CAST(SUM(vendas.qtds) AS BIGINT) AS Qtd,
    SUM(CONVERT(DECIMAL(10,3), vendas.totas)) AS Total_Liq,
    SUM(CONVERT(DECIMAL(10,3), vendas.valrats)) AS Desconto,
    SUM(vendas.VALDESCS - vendas.VALRATS) AS Desconto_validado,
    SUM(CONVERT(DECIMAL(10,3), vendas.custos)) AS Custo,
    SUM(CONVERT(DECIMAL(10,3), vendas.totbrts)) AS Total_Bruto,
    RTRIM(g.codevents) AS Evento,
    RTRIM(ev.desevents) AS Desc_Evento,
    CONCAT(RTRIM(vendas.iclis), '-', CAST(vendas.datas AS DATE)) AS ID_Venda
FROM sljgdmi AS vendas
JOIN SLJGGRP B ON vendas.ggrus = B.codigos AND B.relgers <> 2
LEFT JOIN SLJEMP AS empresa ON empresa.CEMPS = vendas.EMPS
LEFT JOIN sljeest AS g WITH (NOLOCK) ON vendas.empdopnums = g.empdopnums
LEFT JOIN SLJCLI AS consultora ON consultora.ICLIS = vendas.VENDS
LEFT JOIN sljevent AS ev ON g.codevents = ev.codevents
WHERE vendas.tipoops < 90
GROUP BY
    vendas.emps,
    empresa.class,
    vendas.dopes,
    vendas.tipoops,
    vendas.numes,
    vendas.datas,
    vendas.iclis,
    vendas.rclis,
    vendas.vends,
    consultora.RCLIS,
    vendas.ggrus,
    vendas.cpros,
    vendas.codbarras,
    vendas.codtams,
    g.codevents,
    ev.desevents
ORDER BY vendas.datas DESC;