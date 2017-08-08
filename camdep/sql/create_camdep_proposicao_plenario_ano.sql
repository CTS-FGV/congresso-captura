--
-- TABELA PARA PROPOSIÇÕES POR ID
--

DROP TABLE IF EXISTS c_camdep.camdep_proposicoes_plenario_ano CASCADE;

CREATE TABLE c_camdep.camdep_proposicoes_plenario_ano
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  codProposicao integer,
  nomeProposicao character varying(255),
  dataVotacao timestamp without time zone
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_proposicoes_plenario_ano
  OWNER TO pcc_pg001;
