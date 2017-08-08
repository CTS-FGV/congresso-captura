-- CREATE TABLE r_camdep.fid_gov_uf_semana
CREATE TABLE r_camdep.fid_gov_uf_semana AS
SELECT
    CONCAT(EXTRACT(YEAR FROM d.datavotacao)::TEXT,'-',LPAD(EXTRACT(MONTH FROM d.datavotacao)::TEXT,2,'0'),'-',LPAD(EXTRACT(WEEK FROM d.datavotacao)::TEXT,2,'0')) anomessemana,
    d.uf,
    SUM(CASE WHEN b.orientacao = d.voto THEN 1 ELSE 0 END) votos_governo,
    SUM(CASE WHEN b.orientacao <> d.voto THEN 1 ELSE 0 END) votos_contra,
    SUM(CASE WHEN b.orientacao = d.voto THEN 1 ELSE 0 END)+SUM(CASE WHEN b.orientacao <> d.voto THEN 1 ELSE 0 END) votos_total,
    100*(SUM(CASE WHEN b.orientacao = d.voto THEN 1 ELSE 0 END)/(SUM(CASE WHEN b.orientacao = d.voto THEN 1 ELSE 0 END)+SUM(CASE WHEN b.orientacao <> d.voto THEN 1 ELSE 0 END))::DECIMAL(10,6))::DECIMAL(10,4) percent_governo
FROM
    c_camdep.camdep_proposicoes_votacao_bancada b,
    c_camdep.camdep_proposicoes_votacao_deputado d
WHERE
    b.codproposicao = d.codproposicao
    AND b.sigla = 'GOV.'
GROUP BY
    1,2
ORDER BY
    1,6 DESC
LIMIT 0