--
-- TABELA PARA PARTIDOS
--

DROP TABLE IF EXISTS c_camdep.camdep_proposicoes_periodo CASCADE;

CREATE TABLE c_camdep.camdep_proposicoes_periodo
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  codProposicao integer,
  tipoProposicao character varying(255),
  numero integer,
  ano integer,
  dataTramitacao timestamp without time zone,
  dataAlteracao timestamp without time zone
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_proposicoes_periodo
  OWNER TO pcc_pg001;
