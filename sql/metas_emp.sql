SELECT Empresa,
       Data_Inicio,
       Data_Fim,
       MetasEmp
FROM (
    SELECT 
        RTRIM(a.emps) as Empresa,
        CAST(a.dtinis as DATE) as Data_Inicio,
        CAST(a.dtfims as DATE) as Data_Fim,
        CAST(a.metasemp AS integer) as MetasEmp,
        ROW_NUMBER() OVER (
            PARTITION BY a.emps, a.dtinis, a.dtfims
            ORDER BY a.emps
        ) as rn
    FROM sljremvc as a
) t
WHERE rn = 1
ORDER BY Empresa, Data_Inicio;
