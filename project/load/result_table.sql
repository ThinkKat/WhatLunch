CREATE TABLE IF NOT EXISTS car_auctions (
    id              BIGSERIAL PRIMARY KEY,               -- 내부 관리용 PK
    brand           TEXT        NOT NULL,                -- 브랜드 (예: 현대, 기아, 쉐보레)
    model           TEXT        NOT NULL,                -- 모델 (예: 카니발, 올란도, 싼타페 더 프라임)
    trim            TEXT,                                -- 세부 트림/옵션 (예: 시그니처 9인승, LT 프리미엄)
    year            INT         CHECK (year BETWEEN 1950 AND EXTRACT(YEAR FROM CURRENT_DATE)+1),
    transmission    TEXT,                                -- 변속기 (자동, 수동 등)
    fuel            TEXT,                                -- 연료 (가솔린, 디젤, 하이브리드, 전기 등)
    displacement_cc INT         CHECK (displacement_cc >= 0),  -- 배기량
    mileage_km      INT         CHECK (mileage_km >= 0),       -- 주행거리
    color           TEXT,                                -- 색상
    winning_price   BIGINT      CHECK (winning_price >= 0),    -- 낙찰가
    auction_house   TEXT,                                -- 경매장
    auction_date    DATE        NOT NULL,                -- 경매 날짜
    created_at      TIMESTAMPTZ DEFAULT NOW()
);