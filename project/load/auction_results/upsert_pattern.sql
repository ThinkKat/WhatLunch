INSERT INTO auction_results
(brand, model, trim, year, transmission, fuel, displacement_cc, mileage_km, color, winning_price, auction_house, auction_date)
VALUES
(:brand, :model, :trim, :year, :transmission, :fuel, :displacement_cc, :mileage_km, :color, :winning_price, :auction_house, :auction_date)
ON CONFLICT ON CONSTRAINT uq_auction_results_nk
DO UPDATE SET
  brand         = EXCLUDED.brand,
  trim          = EXCLUDED.trim,
  transmission  = EXCLUDED.transmission,
  fuel          = EXCLUDED.fuel,
  color         = EXCLUDED.color,
  winning_price = EXCLUDED.winning_price,
  auction_date  = EXCLUDED.auction_date;
