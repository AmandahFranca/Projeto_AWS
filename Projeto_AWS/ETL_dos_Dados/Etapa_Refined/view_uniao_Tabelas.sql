CREATE OR REPLACE VIEW uniao_tabela_fato_dimensoes AS
SELECT DISTINCT
    f.imdb_id AS id_fato_filme,         
    f.titulo,
    f.popularidade,
    f.nota_media,
    f.numero_voto,
    r.nomeArtista,                       
    r.anoLancamento AS ano_lancamento
FROM 
    fato_filme f
JOIN 
    Refined_csv r
ON 
    f.imdb_id = r.id;