create table c_congresso.parlamentar_autoria
(
	id_autoria text not null PRIMARY KEY,
	id_especie_legislativa text not null,
	id_parlamentar text not null,
	indicador_autor_principal text,
	numero_ordem_autor bigint,
	indicador_outros_autores text,
	url_captura text,
	data_captura timestamp
)
;