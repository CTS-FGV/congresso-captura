-- auto-generated definition
create table c_congresso.estados
(
	uf char(2) not null
		constraint estados_pk
			primary key,
	nome_estado varchar(20),
	regiao varchar(20)
)
;

