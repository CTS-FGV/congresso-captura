-- CREATE TABLE r_camdep.fid_gov_uf_ano
CREATE TABLE r_camdep.fid_gov_uf_ano AS
SELECT
    EXTRACT(YEAR FROM d.datavotacao) ano,
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