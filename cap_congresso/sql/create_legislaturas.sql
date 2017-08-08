create table c_congresso.legislatura
(
	id_legislatura text not null primary key,
	id_api bigint not null,
	sigla_casa text not null,
	numero_legislatura bigint,
	data_inicio date,
	data_fim date,
	data_eleicao date,
	data_captura timestamp,
	url_captura text
);