SELECT 
	schemaname,
	relname tabela,
	n_live_tup registros
FROM
	pg_stat_user_tables
WHERE
	schemaname IN ('capture','a_camdep')
ORDER BY
	relname;

-- VER LOGS
SELECT * FROM c_camdep.camdep_capture_log ORDER BY id DESC;

-- VER TABELAS
SELECT * FROM c_camdep.camdep_deputados_atual;


-- LOG RESUMO
SELECT
	script, datahora, detalhes, quantidade
FROM (
	SELECT
		DISTINCT ON (script)
		script, capnum, datahora, detalhes, quantidade
	FROM c_camdep.camdep_capture_log
	WHERE script IS NOT NULL
	ORDER BY script, id DESC
) tmp
ORDER BY datahora DESC
