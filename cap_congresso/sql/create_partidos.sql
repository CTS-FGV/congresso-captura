create table c_congresso.partidos
(
	sigla text not null primary key,
	nome text,
	data_criacao date,
  data_extincao date,
	data_captura timestamp,
	url_captura text
)
;

