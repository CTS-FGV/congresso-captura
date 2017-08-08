--
-- TABELAS PARA VOTAÇÕES
--

-- VOTAÇÕES PRINCIPAL

DROP TABLE IF EXISTS c_camdep.camdep_proposicoes_votacao CASCADE;

CREATE TABLE c_camdep.camdep_proposicoes_votacao
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  codProposicao integer,
  tipo character varying(255),
  numero integer,
  ano integer,
  codSessao integer,
  dataVotacao timestamp without time zone,
  Resumo character varying(20000),
  objVotacao character varying(2001)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_proposicoes_votacao
  OWNER TO pcc_pg001;


-- ORIENTAÇÃO BANCADA

DROP TABLE IF EXISTS c_camdep.camdep_proposicoes_votacao_bancada CASCADE;

CREATE TABLE c_camdep.camdep_proposicoes_votacao_bancada
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  codProposicao integer,
  tipo character varying(255),
  numero integer,
  ano integer,
  codSessao integer,
  dataVotacao timestamp without time zone,
  Sigla character varying(255),
  orientacao character varying(255)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_proposicoes_votacao_bancada
  OWNER TO pcc_pg001;


-- VOTAÇÃO DEPUTADOS

DROP TABLE IF EXISTS c_camdep.camdep_proposicoes_votacao_deputado CASCADE;

CREATE TABLE c_camdep.camdep_proposicoes_votacao_deputado
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  codProposicao integer,
  tipo character varying(255),
  numero integer,
  ano integer,
  codSessao integer,
  dataVotacao timestamp without time zone,
  ideCadastro int,
  Nome character varying(255),
  Partido character varying(255),
  UF character varying(255),
  voto character varying(255)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_proposicoes_votacao_deputado
  OWNER TO pcc_pg001;

CREATE INDEX idx_camdep_proposicoes_votacao_deputado_nome
   ON c_camdep.camdep_proposicoes_votacao_deputado (nome ASC NULLS LAST);

CREATE INDEX idx_camdep_proposicoes_votacao_deputado_codproposicao
   ON c_camdep.camdep_proposicoes_votacao_deputado (codproposicao ASC NULLS LAST);

CREATE INDEX idx_camdep_proposicoes_votacao_deputado_datavotacao
   ON c_camdep.camdep_proposicoes_votacao_deputado (datavotacao ASC NULLS LAST);
