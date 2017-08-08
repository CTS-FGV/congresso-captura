-- Reference: parlamentar_detalhe_estados (table: parlamentar_detalhe)
ALTER TABLE parlamentar_detalhe ADD CONSTRAINT parlamentar_detalhe_estados
    FOREIGN KEY (sigla_uf_origem)
    REFERENCES estados (uf)
    NOT DEFERRABLE
    INITIALLY IMMEDIATE
;