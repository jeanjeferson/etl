WITH
    Taxas
    AS
    (
        SELECT
            a.valos,
            a.cmoes
        FROM sljcot a (NOLOCK)
            JOIN (
                    SELECT
                MAX(datas) AS datas,
                cmoes
            FROM sljcot (NOLOCK)
            GROUP BY cmoes
                ) b ON a.cmoes = b.cmoes AND a.datas = b.datas
        GROUP BY a.valos, a.cmoes
    )

SELECT
    RTRIM(a.emps) AS Empresa,
    RTRIM(a.grupos) AS Cod_Gr_Est,
    MAX(RTRIM(d.descrs)) AS Desc_Gr_Est,
    RTRIM(a.estos) AS Cod_Cta_Est,
    MAX(RTRIM(c.rclis)) AS Desc_Cta_Est,
    RTRIM(d.tipoinvs) AS Tipo_inventario,
    RTRIM(a.cunis) AS Unidade,
    RTRIM(a.cpros) AS Cod_Prod,
    RTRIM(b.pcuss) AS Custo_Produto,
    RTRIM(b.pvens) AS Preco_Venda,
    CAST(SUM(a.sqtds) AS INTEGER) AS Qtd,
    SUM(a.sqtds * b.custofs * ISNULL(t.valos, 1)) AS Custo_Real,
    SUM(a.sqtds * b.pvens * ISNULL(t.valos, 1)) AS Pr_Venda_Real
FROM sljest a
    LEFT JOIN sljpro b ON a.cpros = b.cpros
    LEFT JOIN sljcli c ON a.estos = c.iclis
    LEFT JOIN sljgccr d ON a.grupos = d.codigos
    LEFT JOIN Taxas t ON b.moecusfs = t.cmoes
WHERE d.tipoinvs IN (1, 2)
GROUP BY
    a.emps,
    a.grupos,
    a.estos,
    d.tipoinvs,
    a.cunis,
    a.cpros,
    b.pcuss,
    b.pvens
