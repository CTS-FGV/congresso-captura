--
-- TABELA PARA DEPUTADOS HISTÓRICO
--

DROP TABLE IF EXISTS c_camdep.camdep_deputados_historico CASCADE;

CREATE TABLE c_camdep.camdep_deputados_historico
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  idecadastro integer,
  numlegislatura integer,
  nomeparlamentar character varying(255),
  sexo character varying(255),
  profissao character varying(255),
  legendapartidoeleito character varying(255),
  ufeleito character varying(255),
  condicao character varying(255),
  situacaomandato character varying(255),
  matricula integer,
  gabinete character varying(255),
  anexo character varying(255),
  fone character varying(255)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_deputados_historico
  OWNER TO pcc_pg001;
