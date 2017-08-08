--
-- TABELA DE LOG
--

DROP TABLE IF EXISTS c_camdep.camdep_capture_log CASCADE;

CREATE TABLE c_camdep.camdep_capture_log
(
  id bigserial NOT NULL,
  capnum bigint,
  script character varying(255),
  datahora timestamp without time zone,
  detalhes character varying(1024),
  quantidade bigint
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_capture_log
  OWNER TO pcc_pg001;

INSERT INTO c_camdep.camdep_capture_log (capnum) VALUES (0);
