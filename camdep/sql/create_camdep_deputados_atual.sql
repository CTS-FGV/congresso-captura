--
-- TABELA PARA DEPUTADOS ATUAIS
--

DROP TABLE IF EXISTS c_camdep.camdep_deputados_atual CASCADE;

CREATE TABLE c_camdep.camdep_deputados_atual
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  idecadastro integer,
  condicao character varying(255),
  matricula integer,
  idparlamentar integer,
  nome character varying(255),
  nomeparlamentar character varying(255),
  urlfoto character varying(255),
  sexo character varying(255),
  uf character varying(255),
  partido character varying(255),
  gabinete character varying(255),
  anexo character varying(255),
  fone character varying(255),
  email character varying(255)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_deputados_atual
  OWNER TO pcc_pg001;
