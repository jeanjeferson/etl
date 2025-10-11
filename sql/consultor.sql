SELECT DISTINCT
    COALESCE(RTRIM(a.grupos), '') AS Grupo,
    COALESCE(RTRIM(vendas.vends), '') AS Cod_Funcionario,
    COALESCE(RTRIM(a.rclis), 'Sem Vendedor') AS Nome
FROM sljgdmi vendas
LEFT JOIN sljcli a ON a.iclis = vendas.vends



-- SELECT grupos, iclis, rclis FROM sljcli