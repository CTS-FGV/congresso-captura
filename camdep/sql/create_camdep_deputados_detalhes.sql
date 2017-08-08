-- ------------------------------------------------
-- TABELA PARA DEPUTADOS DETALHES
-- ------------------------------------------------

DROP TABLE IF EXISTS c_camdep.camdep_deputados_detalhes CASCADE;

CREATE TABLE c_camdep.camdep_deputados_detalhes
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  numlegislatura integer,
  email character varying(255),
  nomeprofissao character varying(255),
  datanascimento date,
  datafalecimento date,
  ufrepresentacaoatual character varying(255),
  situacaonalegislaturaatual character varying(255),
  idecadastro integer,
  idparlamentardeprecated integer,
  nomeparlamentaratual character varying(255),
  nomecivil character varying(255),
  sexo character varying(255),
  partidoatualid character varying(255),
  partidoatualsigla character varying(255),
  partidoatualnome character varying(255),
  gabinetenumero character varying(255),
  gabineteanexo character varying(255),
  gabinetetelefone character varying(255)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_deputados_detalhes
  OWNER TO pcc_pg001;

--
-- TABELA PARA DEPUTADOS DETALHES - COMISSÕES
--

DROP TABLE IF EXISTS c_camdep.camdep_deputados_detalhes_comissoes CASCADE;

CREATE TABLE c_camdep.camdep_deputados_detalhes_comissoes
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  numlegislatura integer,
  idecadastro integer,
  idOrgaoLegislativoCD integer,
  siglaComissao character varying(255),
  nomeComissao character varying(1024),
  condicaoMembro character varying(255),
  dataEntrada date,
  dataSaida date
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_deputados_detalhes_comissoes
  OWNER TO pcc_pg001;

--
-- TABELA PARA DEPUTADOS DETALHES - CARGOS COMISSÕES
--

DROP TABLE IF EXISTS c_camdep.camdep_deputados_detalhes_comissoes_cargos CASCADE;

CREATE TABLE c_camdep.camdep_deputados_detalhes_comissoes_cargos
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  numlegislatura integer,
  idecadastro integer,
  idOrgaoLegislativoCD integer,
  siglaComissao character varying(255),
  nomeComissao character varying(1024),
  idcargo integer,
  nomecargo character varying(255),
  dataEntrada date,
  dataSaida date
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_deputados_detalhes_comissoes_cargos
  OWNER TO pcc_pg001;

--
-- TABELA PARA DEPUTADOS DETALHES - PERIODOS EXERCICIO
--

DROP TABLE IF EXISTS c_camdep.camdep_deputados_detalhes_periodos_exercicio CASCADE;

CREATE TABLE c_camdep.camdep_deputados_detalhes_periodos_exercicio
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  numlegislatura integer,
  idecadastro integer,
  siglaUFRepresentacao character varying(255),
  situacaoExercicio character varying(255),
  dataInicio date,
  dataFim date,
  idCausaFimExercicio integer,
  descricaoCausaFimExercicio character varying(255),
  idCadastroParlamentarAnterior integer
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_deputados_detalhes_periodos_exercicio
  OWNER TO pcc_pg001;

--
-- TABELA PARA DEPUTADOS DETALHES - FILIACOES PARTIDARIAS
--

DROP TABLE IF EXISTS c_camdep.camdep_deputados_detalhes_filiacoes CASCADE;

CREATE TABLE c_camdep.camdep_deputados_detalhes_filiacoes
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  numlegislatura integer,
  idecadastro integer,
  idPartidoAnterior character varying(255),
  siglaPartidoAnterior character varying(255),
  nomePartidoAnterior character varying(255),
  idPartidoPosterior character varying(255),
  siglaPartidoPosterior character varying(255),
  nomePartidoPosterior character varying(255),
  dataFiliacaoPartidoPosterior date
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_deputados_detalhes_periodos_exercicio
  OWNER TO pcc_pg001;

--
-- TABELA PARA DEPUTADOS DETALHES - LIDERANCAS
--

DROP TABLE IF EXISTS c_camdep.camdep_deputados_detalhes_lideranca CASCADE;

CREATE TABLE c_camdep.camdep_deputados_detalhes_lideranca
(
  id bigserial,
  capnum bigint,
  datacaptura timestamp without time zone,
  numlegislatura integer,
  idecadastro integer,
  idHistoricoLider integer,
  idCargoLideranca character varying(255),
  descricaoCargoLideranca character varying(255),
  numOrdemCargo integer,
  dataDesignacao date,
  dataTermino date,
  codigoUnidadeLideranca character varying(255),
  siglaUnidadeLideranca character varying(255),
  idBlocoPartido character varying(255)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE c_camdep.camdep_deputados_detalhes_lideranca
  OWNER TO pcc_pg001;
