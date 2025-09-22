Giorno 1 – Ingestion (Bronze)
Obiettivo: Caricare i dati grezzi (5 file CSV) nella Bronze Layer.
Attività richieste:
• Leggere i 5 file CSV (hotels, rooms, customers, bookings, payments).
• Salvare i dati in formato CSV/Parquet in una cartella &#39;bronze/&#39;.
• Aggiungere una colonna ingestion_date con la data di caricamento.
• Non effettuare alcuna pulizia in questa fase.
Deliverable: 5 tabelle Bronze con i dati originali, comprensivi di errori e anomalie.
Giorno 2 – Pulizia (Silver)
Obiettivo: Applicare regole di data quality e produrre dati puliti (Silver Layer).
Attività richieste:
• Customers: sostituire email vuote con NULL, rimuovere duplicati per customer_id.
• Rooms: eliminare duplicati per room_id.
• Bookings: correggere date invertite (scambiare checkin/checkout), sostituire importi
negativi con NULL, accettare solo currency valide (EUR, USD, GBP).
• Payments: marcare righe con booking_id non esistente come orphan, marcare righe con
amount &gt; total_amount come over_amount, sostituire currency non valide con NULL.
Deliverable: 5 tabelle Silver con dati puliti e colonne dq_* per segnalare anomalie.
Giorno 3 – KPI di Business (Gold)
Obiettivo: Creare tabelle Gold di sintesi a supporto del business.
Attività richieste:
1. Daily Revenue – sommare i ricavi per giorno (solo prenotazioni valide e confermate).
Colonne: date, gross_revenue, bookings_count.
2. Cancellation Rate by Source – calcolare % cancellazioni per canale (source).
Colonne: source, total_bookings, cancelled, cancellation_rate_pct.
3. Collection Rate – calcolare rapporto incassi (payments validi) / totale prenotazioni.
Colonne: hotel_id, total_bookings_value, total_payments_value, collection_rate.

4. Overbooking Alerts – segnalare camere con prenotazioni sovrapposte.
Colonne: room_id, booking_id_1, booking_id_2, overlap_start, overlap_end.
5. Customer Value – analizzare valore clienti (numero prenotazioni, spesa totale, media).
Colonne: customer_id, bookings_count, revenue_sum, avg_ticket.
Deliverable: 5 tabelle Gold (CSV/Parquet/Delta) con KPI calcolati e notebook/script con la
logica.
Giorno 4 – Orchestrazione &amp; Big Data
Obiettivo: Automatizzare la pipeline e validare le trasformazioni su Big Data.
Attività richieste:
• Creare un DAG in Airflow (o pipeline in ADF) con step: Bronze → Silver → Gold.
• Implementare almeno una trasformazione Silver e una Gold in Spark/Databricks.
• Salvare i dati Silver/Gold in formato ottimizzato (Delta o Parquet).
Deliverable: un DAG funzionante e almeno un notebook Spark/Databricks con esempi
pratici.
Giorno 5 – Machine Learning &amp; Reportistica
Obiettivo: Integrare un modello ML e produrre report finali.
Attività richieste:
• Addestrare un modello ML di regressione: prevedere il costo totale di una prenotazione
in base a variabili come durata del soggiorno, numero ospiti, tipologia camera e periodo
dell’anno.
• Integrare il risultato come nuova colonna nei dati (predicted_price).
• Generare un report (HTML/PDF o dashboard) con i KPI Gold.
• Preparare slide finali con architettura, KPI, anomalie e raccomandazioni business.
Deliverable: modello ML salvato, report con grafici, presentazione conclusiva.

Valutazione
Gli studenti saranno valutati su:
- Correttezza ingestion (Bronze)
- Applicazione regole DQ (Silver)
- Calcolo KPI richiesti (Gold)
- Orchestrazione pipeline (Airflow/ADF)
- Integrazione ML &amp; Reportistica
- Documentazione e presentazione finale

Extra (facoltativi)
• Definire SLA di qualità (es. fallimento pipeline se &gt;5% anomalie).
• Prevedere tasso di cancellazione con modello ML semplice.
• Usare Generative AI per generare automaticamente executive summary.