-- ETL Ordini - Caricamento dati ordini e righe ordine
-- Versione: 1.0
-- Autore: Team ETL
-- Data: 2025-01-15

-- Step 1: Estrazione ordini
CREATE OR REPLACE TEMPORARY TABLE tmp_ordini_raw AS
SELECT 
    o.id_ordine,
    o.data_ordine,
    o.id_cliente,
    o.stato_ordine,
    o.metodo_pagamento,
    o.id_corriere,
    c.nome,
    c.cognome,
    c.email,
    c.telefono
FROM 
    sorgente.ordini o
    LEFT JOIN sorgente.clienti c ON o.id_cliente = c.id_cliente
WHERE 
    DATE(o.data_ordine) >= CURRENT_DATE() - 1;

-- Step 2: Estrazione righe ordine con aggregazioni
CREATE OR REPLACE TEMPORARY TABLE tmp_righe_ordine AS
SELECT 
    ro.id_ordine,
    COUNT(DISTINCT ro.id_prodotto) AS num_prodotti,
    SUM(ro.quantita) AS quantita_totale,
    SUM(ro.prezzo_unitario * ro.quantita) AS importo_totale,
    STRING_AGG(p.nome_prodotto, ', ') AS lista_prodotti
FROM 
    sorgente.righe_ordine ro
    INNER JOIN sorgente.prodotti p ON ro.id_prodotto = p.id_prodotto
GROUP BY 
    ro.id_ordine;

-- Step 3: Join e trasformazioni
CREATE OR REPLACE TEMPORARY TABLE tmp_ordini_processed AS
SELECT 
    o.id_ordine,
    o.data_ordine,
    o.id_cliente,
    CONCAT(o.nome, ' ', o.cognome) AS cliente_completo,
    o.email,
    o.telefono,
    o.stato_ordine,
    o.metodo_pagamento,
    o.id_corriere,
    COALESCE(r.num_prodotti, 0) AS num_prodotti,
    COALESCE(r.quantita_totale, 0) AS quantita_totale,
    COALESCE(r.importo_totale, 0) AS importo_totale,
    r.lista_prodotti,
    CASE 
        WHEN o.stato_ordine = 'COMPLETATO' THEN 1
        ELSE 0
    END AS flag_completato,
    CURRENT_TIMESTAMP() AS data_caricamento
FROM 
    tmp_ordini_raw o
    LEFT JOIN tmp_righe_ordine r ON o.id_ordine = r.id_ordine;

-- Step 4: Caricamento nella tabella target
MERGE INTO dwh.fact_ordini AS target
USING tmp_ordini_processed AS source
ON target.id_ordine = source.id_ordine
WHEN MATCHED THEN
    UPDATE SET
        stato_ordine = source.stato_ordine,
        num_prodotti = source.num_prodotti,
        quantita_totale = source.quantita_totale,
        importo_totale = source.importo_totale,
        flag_completato = source.flag_completato,
        data_aggiornamento = source.data_caricamento
WHEN NOT MATCHED THEN
    INSERT (
        id_ordine,
        data_ordine,
        id_cliente,
        cliente_completo,
        email,
        telefono,
        stato_ordine,
        metodo_pagamento,
        id_corriere,
        num_prodotti,
        quantita_totale,
        importo_totale,
        lista_prodotti,
        flag_completato,
        data_caricamento
    )
    VALUES (
        source.id_ordine,
        source.data_ordine,
        source.id_cliente,
        source.cliente_completo,
        source.email,
        source.telefono,
        source.stato_ordine,
        source.metodo_pagamento,
        source.id_corriere,
        source.num_prodotti,
        source.quantita_totale,
        source.importo_totale,
        source.lista_prodotti,
        source.flag_completato,
        source.data_caricamento
    );