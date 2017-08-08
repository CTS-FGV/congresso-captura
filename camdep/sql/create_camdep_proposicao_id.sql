--
-- TABELA PARA PROPOSIÇÕES POR ID
--

DROP TABLE IF EXISTS c_camdep.camdep_proposicoes_id CASCADE;

CREATE TABLE c_camdep.camdep_proposicoes_id
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  nomeProposicao character varying(512),
  idProposicao integer,
  idProposicaoPrincipal character varying(2001),
  nomeProposicaoOrigem character varying(512),
  tipoProposicao character varying(512),
  tema character varying(512),
  Ementa character varying(20002),
  ExplicacaoEmenta character varying(20002),
  Autor character varying(2000),
  ideCadastro integer,
  ufAutor character varying(512),
  partidoAutor character varying(512),
  DataApresentacao timestamp without time zone,
  RegimeTramitacao character varying(512),
  UltimoDespacho character varying(20000),
  Apreciacao character varying(2003),
  Indexacao character varying(20001),
  Situacao character varying(512),
  LinkInteiroTeor character varying(512),
  apensadas character varying(512)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_proposicoes_id
  OWNER TO pcc_pg001;
