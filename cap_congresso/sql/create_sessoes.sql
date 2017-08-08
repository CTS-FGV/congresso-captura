create table c_congresso.sessoes
(
	id_sessao text not null PRIMARY KEY ,
  id_api text not null,
  sigla_casa text not null,
  datetime_sessao timestamp,
  numero_sessao bigint,
  tipo_sessao text,
  descricao_tipo_sessao text,
  id_sessao_legislativa text,
  data_captura timestamp,
  url_captura text,
	api_captura text
);