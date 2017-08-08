--
-- TABELA PARA LISTA DE DISCURSOS
--

DROP TABLE IF EXISTS c_camdep.camdep_discursos_lista CASCADE;

CREATE TABLE c_camdep.camdep_discursos_lista
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  codigo character varying(255),
  data timestamp without time zone,
  numero integer,
  tipo character varying(500),
  fasecodigo character varying(255),
  fasedescricao character varying(255),
  fasenumero integer,
  nome character varying(255),
  partido character varying(255),
  uf character varying(255),
  horainiciodiscurso timestamp without time zone,
  txtindexacao character varying(20000),
  numeroquarto integer,
  numeroinsercao integer,
  sumario character varying(20000)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_discursos_lista
  OWNER TO pcc_pg001;
