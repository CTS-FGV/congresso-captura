
-- TABELA PARA PRESENCA DOS DEPUTADOS

DROP TABLE IF EXISTS c_camdep.camdep_listarpresencasdia CASCADE;

CREATE TABLE c_camdep.camdep_listarpresencasdia (
   id bigserial,
   capnum bigint,
   datacaptura timestamp,
   legislatura int,
   carteiraParlamentar int,
   nomeParlamentar varchar(255),
   siglaPartido varchar(10),
   siglaUF char(2),
   dia varchar(255),
   mes varchar(255),
   ano varchar(255),
   hora varchar(255),
   descricaoSessao varchar(255),
   frequencia varchar(255)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_listarpresencasdia
  OWNER TO pcc_pg001;
