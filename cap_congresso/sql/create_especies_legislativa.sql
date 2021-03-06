create table c_congresso.especies_legislativas
(
	id_especie_legislativa text PRIMARY KEY NOT NULL,
	id_api bigint not null,
	api_captura text not null,
	sigla_casa_identificacao text,
	nome_casa_identificacao text,
	sigla_subtipo text,
	descricao_subtipo text,
	numero bigint,
	ano bigint,
	indicador_tramitando text,
	ementa text,
	explicacao_ementa text,
	obsercacao text,
	apelido text,
	indexacao text,
	indicador_complementar text,
	data_apresentacao text,
	data_leitura date,
	sigla_casa_leitura text,
	nome_poder_origem text,
	sigla_casa_origem text,
	nome_casa_origem text,
	sigla_casa_iniciadora text,
	nome_casa_iniciadora text,
	data_captura timestamp,
	nome_casa_leitura text,
	url_captura text
)
;