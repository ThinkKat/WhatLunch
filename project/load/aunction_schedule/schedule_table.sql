
CREATE TABLE public.auction_schedules (
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
    min_price       BIGINT NOT NULL CHECK (min_price >= 0),
    auction_house   TEXT NOT NULL,
    auction_date    DATE NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_auction_schedules_nk UNIQUE (model, year, displacement_cc, mileage_km, auction_house, auction_date)
);

CREATE INDEX idx_auction_schedules_brand_year
    ON public.auction_schedules (brand, year);

CREATE INDEX idx_auction_schedules_auction_date
    ON public.auction_schedules (auction_date);

CREATE INDEX idx_auction_schedules_price
    ON public.auction_schedules (winning_price);
