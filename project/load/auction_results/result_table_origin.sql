-- 1. 기존 테이블 삭제 (존재하면)
DROP TABLE IF EXISTS public.auction_results CASCADE;

-- 2. 새 테이블 생성
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

    -- 새로운 UNIQUE 제약: 날짜까지 포함
    CONSTRAINT uq_auction_results_nk UNIQUE (model, year, displacement_cc, mileage_km, auction_house, auction_date)
);

-- 3. 인덱스 추가 (조회 성능용, 필요 시)
CREATE INDEX idx_auction_results_brand_year
    ON public.auction_results (brand, year);

CREATE INDEX idx_auction_results_auction_date
    ON public.auction_results (auction_date);

CREATE INDEX idx_auction_results_price
    ON public.auction_results (winning_price);
