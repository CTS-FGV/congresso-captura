DROP TABLE IF EXISTS a_camdep.votacao_bancada;

CREATE TABLE a_camdep.votacao_bancada AS
SELECT DISTINCT 
    codproposicao, tipo, numero, ano, codsessao, 
    datavotacao, sigla, orientacao
FROM c_camdep.camdep_proposicoes_votacao_bancada;

DROP TABLE IF EXISTS a_camdep.votacao_deputado;

CREATE TABLE a_camdep.votacao_deputado AS
SELECT DISTINCT 
    codproposicao, tipo, numero, ano, codsessao, 
    datavotacao, idecadastro, nome, partido, uf, voto
FROM c_camdep.camdep_proposicoes_votacao_deputado;
