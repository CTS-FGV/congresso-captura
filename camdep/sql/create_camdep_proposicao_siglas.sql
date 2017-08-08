--
-- TABELA PARA PARTIDOS
--

DROP TABLE IF EXISTS c_camdep.camdep_proposicoes_siglas CASCADE;

CREATE TABLE c_camdep.camdep_proposicoes_siglas
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  tiposigla character varying(255),
  descricao character varying(255),
  ativa boolean,
  genero character varying(255)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_proposicoes_siglas
  OWNER TO pcc_pg001;
