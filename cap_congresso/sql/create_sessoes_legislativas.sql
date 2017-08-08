create table c_congresso.sessoes_legislativas
(
	id_sessao_legislativa text not null primary key,
	sigla_casa text not null,
  data_inicio date,
	data_fim date,
	data_inicio_intervalo date,
	data_fim_intervalo date,
  tipo text,
  id_legislatura text,
	data_captura timestamp,
	url_captura text
);