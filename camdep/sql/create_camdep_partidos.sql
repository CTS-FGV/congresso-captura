--
-- TABELA PARA PARTIDOS
--

DROP TABLE IF EXISTS c_camdep.camdep_partidos CASCADE;

CREATE TABLE c_camdep.camdep_partidos
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  idPartido character varying(255),
  siglaPartido character varying(255),
  nomePartido character varying(255),
  dataCriacao date,
  dataExtincao date
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_partidos
  OWNER TO pcc_pg001;
