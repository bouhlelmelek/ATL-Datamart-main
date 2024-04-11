-- Dimension Temporelle
CREATE TABLE DimensionTemporelle (
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    PRIMARY KEY (pickup_datetime, dropoff_datetime) -- Ajoutez d'autres colonnes si nécessaire
);
-- Dimension Lieu
CREATE TABLE DimensionLieu (
    pulocationid INT,
    dolocationid INT,
    PRIMARY KEY (pulocationid, dolocationid) -- Ajoutez d'autres colonnes si nécessaire
);
-- Dimension Fournisseur
CREATE TABLE DimensionFournisseur (
    vendorid INT,
    PRIMARY KEY (vendorid) -- Ajoutez d'autres colonnes si nécessaire
);
-- Dimension Tarification
CREATE TABLE DimensionTarification (
    ratecodeid INT,
    PRIMARY KEY (ratecodeid) -- Ajoutez d'autres colonnes si nécessaire
);
-- Dimension Paiement
CREATE TABLE DimensionPaiement (
    payment_type INT,
    PRIMARY KEY (payment_type) -- Ajoutez d'autres colonnes si nécessaire
);
-- Table des Faits
CREATE TABLE FactTable (
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    pulocationid INT,
    dolocationid INT,
    vendorid INT,
    ratecodeid INT,
    payment_type INT,
    trip_distance NUMERIC(53, 3),
    fare_amount NUMERIC(53, 3),
    extra NUMERIC(53, 3),
    mta_tax NUMERIC(53, 3),
    tip_amount NUMERIC(53, 3),
    tolls_amount NUMERIC(53, 3),
    improvement_surcharge NUMERIC(53, 3),
    total_amount NUMERIC(53, 3),
    congestion_surcharge NUMERIC(53, 3),
    airport_fee NUMERIC(53, 3),
    PRIMARY KEY (
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        vendorid,
        ratecodeid,
        payment_type
    ),
    FOREIGN KEY (pickup_datetime, dropoff_datetime) REFERENCES DimensionTemporelle(pickup_datetime, dropoff_datetime),
    FOREIGN KEY (pulocationid, dolocationid) REFERENCES DimensionLieu(pulocationid, dolocationid),
    FOREIGN KEY (vendorid) REFERENCES DimensionFournisseur(vendorid),
    FOREIGN KEY (ratecodeid) REFERENCES DimensionTarification(ratecodeid),
    FOREIGN KEY (payment_type) REFERENCES DimensionPaiement(payment_type)
);