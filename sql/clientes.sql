SELECT
    RTRIM(a.contaprinc) AS Cod_Conta_Principal,
    RTRIM(COALESCE(b.nome, a.rclis)) AS Conta_Principal,
    RTRIM(a.grupos) AS Grupo,
    RTRIM(a.iclis) AS Cod_Cliente,
    RTRIM(a.rclis) AS Nome,
    RTRIM(a.sexos) AS Sexo,
    RTRIM(a.estcivils) AS Estado_Civil,
    RTRIM(a.cpfs) AS CPF,
    CAST(a.nascs AS DATE) AS Data_Nascimento,
    DATEDIFF(YEAR, a.nascs, GETDATE()) AS Idade,
    CAST(a.dtcasas AS DATE) AS Data_Casamento,
    DATEDIFF(YEAR, a.dtcasas, GETDATE()) AS Idade_Casamento,
    RTRIM(a.contavens) AS Cod_Vend,
    RTRIM(k.rclis) AS Consultora,
    RTRIM(a.ddds) AS DDD,
    RTRIM(a.faxs) AS Cel,
    RTRIM(a.tel1s) AS Tel1,
    RTRIM(a.tel2s) AS Tel2,
    RTRIM(a.tel3s) AS Tel3,
    LOWER(RTRIM(a.emails)) AS Email,
    RTRIM(a.conjuges) AS Conjuge,
    RTRIM(a.cpfcs) AS CPF_Conjuge,
    RTRIM(a.ceps) AS CEP,
    RTRIM(a.endes) AS Endereco,
    RTRIM(a.nums) AS Numero,
    RTRIM(a.compls) AS Complemento,
    RTRIM(a.bairs) AS Bairro,
    RTRIM(a.cidas) AS Cidade,
    RTRIM(a.estas) AS Estado,
    RTRIM(a.paises) AS Pais,
    RTRIM(a.fpubls) AS VIP,
    CAST(a.ultcomps AS DATE) AS Ultima_Compra,
    CAST(a.dataincs AS DATE) AS Data_Inclusao,
    YEAR(GETDATE()) - YEAR(a.dataincs) AS Anos_Marca,
    CAST(a.dtalts AS DATE) AS Data_Alteracao,
    a.ctelems,
    CASE
        WHEN a.inativas = 0 THEN 'Ativo'
        ELSE 'Inativo'
    END AS 'Status',
    DAY(a.nascs) AS Dia_Nascimento,
    MONTH(a.nascs) AS Mes_Nascimento
FROM sljcli a
    LEFT JOIN SLJCLI AS k ON k.ICLIS = A.contavens
    LEFT JOIN (SELECT
        RTRIM(a.iclis) AS Cod_Cliente,
        RTRIM(a.rclis) AS Nome
    FROM sljcli a) AS b ON b.Cod_Cliente = a.contaprinc


