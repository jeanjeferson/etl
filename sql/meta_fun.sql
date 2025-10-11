SELECT Empresa,
       Data_Inicio,
       Data_Fim,
       Usuario,
       Cota
FROM (
    SELECT 
        RTRIM(a.emps) as Empresa,
        CAST(a.datinis as DATE) as Data_Inicio,
        CAST(a.datfins as DATE) as Data_Fim,
        RTRIM(a.usuars) as Usuario,
        CAST(a.cotas AS integer) as Cota,
        ROW_NUMBER() OVER (
            PARTITION BY a.emps, a.usuars, a.datinis, a.datfins
            ORDER BY a.emps
        ) as rn
    FROM sljremvd as a
) t
WHERE rn = 1
ORDER BY Empresa, Usuario, Data_Inicio;