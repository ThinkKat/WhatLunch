
CREATE TABLE public.auction_results (
    id              BIGSERIAL PRIMARY KEY,
    brand           TEXT        NOT NULL,
    model           TEXT        NOT NULL,
    trim            TEXT,
    year            INT NOT NULL CHECK (year BETWEEN 1950 AND EXTRACT(YEAR FROM CURRENT_DATE)::INT + 1),
    transmission    TEXT,
    fuel            TEXT,
    displacement_cc INT NOT NULL CHECK (displacement_cc >= 0),
    mileage_km      INT NOT NULL CHECK (mileage_km >= 0),
    color           TEXT,
    winning_price   BIGINT NOT NULL CHECK (winning_price >= 0),
    auction_house   TEXT NOT NULL,
    auction_date    DATE NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_auction_results_nk UNIQUE (model, year, displacement_cc, mileage_km, auction_house, auction_date)
);

