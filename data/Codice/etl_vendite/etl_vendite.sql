-- ETL Vendite - Caricamento dati vendite giornaliere
-- Versione: 1.0
-- Autore: Team ETL
-- Data: 2025-01-15

-- Step 1: Estrazione dati sorgente
CREATE OR REPLACE TEMPORARY TABLE tmp_vendite_raw AS
SELECT 
    v.id_vendita,
    v.data_vendita,
    v.id_cliente,
    v.id_prodotto,
    v.quantita,
    v.prezzo_unitario,
    v.sconto,
    v.id_negozio,
    c.nome AS nome_cliente,
    c.cognome AS cognome_cliente,
    c.regione,
    p.nome_prodotto,
    p.categoria
FROM 
    sorgente.vendite v
    INNER JOIN sorgente.clienti c ON v.id_cliente = c.id_cliente
    INNER JOIN sorgente.prodotti p ON v.id_prodotto = p.id_prodotto
WHERE 
    DATE(v.data_vendita) = CURRENT_DATE() - 1
    AND v.stato = 'CONFERMATA';

-- Step 2: Calcolo metriche
CREATE OR REPLACE TEMPORARY TABLE tmp_vendite_processed AS
SELECT 
    id_vendita,
    data_vendita,
    id_cliente,
    CONCAT(nome_cliente, ' ', cognome_cliente) AS cliente_completo,
    regione,
    id_prodotto,
    nome_prodotto,
    categoria,
    quantita,
    prezzo_unitario,
    sconto,
    -- Calcolo importo totale
    (quantita * prezzo_unitario) AS importo_lordo,
    (quantita * prezzo_unitario * sconto / 100) AS importo_sconto,
    (quantita * prezzo_unitario) - (quantita * prezzo_unitario * sconto / 100) AS importo_netto,
    id_negozio,
    CURRENT_TIMESTAMP() AS data_caricamento
FROM 
    tmp_vendite_raw;

-- Step 3: Caricamento nella tabella target
INSERT INTO dwh.fact_vendite (
    id_vendita,
    data_vendita,
    id_cliente,
    cliente_completo,
    regione,
    id_prodotto,
    nome_prodotto,
    categoria,
    quantita,
    prezzo_unitario,
    sconto,
    importo_lordo,
    importo_sconto,
    importo_netto,
    id_negozio,
    data_caricamento
)
SELECT 
    id_vendita,
    data_vendita,
    id_cliente,
    cliente_completo,
    regione,
    id_prodotto,
    nome_prodotto,
    categoria,
    quantita,
    prezzo_unitario,
    sconto,
    importo_lordo,
    importo_sconto,
    importo_netto,
    id_negozio,
    data_caricamento
FROM 
    tmp_vendite_processed;

-- Step 4: Log esecuzione
INSERT INTO dwh.etl_log (
    nome_etl,
    data_esecuzione,
    record_processati,
    stato
)
SELECT 
    'ETL_VENDITE' AS nome_etl,
    CURRENT_TIMESTAMP() AS data_esecuzione,
    COUNT(*) AS record_processati,
    'SUCCESS' AS stato
FROM 
    tmp_vendite_processed;