CREATE TABLE "dim_bursacomp" (
  "company_key" int PRIMARY KEY,
  "companyfullname" varchar,
  "stockshortname" varchar,
  "stockcode" varchar
);

CREATE TABLE "dim_market" (
  "market_key" int PRIMARY KEY,
  "marketname" varchar 
);

CREATE TABLE "dim_shariahcompliance" (
  "shariah_key" int PRIMARY KEY,
  "shariah" varchar NOT NULL
);

CREATE TABLE "dim_sector" (
  "sector_key" int PRIMARY KEY,
  "sector" varchar NOT NULL
);

CREATE TABLE "dim_date" (
  "date_key" int PRIMARY KEY,
  "loaddate" date NOT NULL,
  "day" int NOT NULL,
  "dayofweek" varchar NOT NULL,
  "month" varchar NOT NULL,
  "quarter" int NOT NULL,
  "year" int NOT NULL
);

CREATE TABLE "fact_marketwatch" (
  "id" int PRIMARY KEY,
  "company_key" int,
  "market_key" int,
  "shariah_key" int,
  "sector_key" int,
  "marketcap" varchar,
  "lastprice" numeric,
  "priceearningratio" numeric,
  "dividentyield" numeric,
  "returnonequity" numeric,
  "loaddate_key" int
);

ALTER TABLE "fact_marketwatch" ADD FOREIGN KEY ("company_key") REFERENCES "dim_bursacomp" ("company_key");

ALTER TABLE "fact_marketwatch" ADD FOREIGN KEY ("market_key") REFERENCES "dim_market" ("market_key");

ALTER TABLE "fact_marketwatch" ADD FOREIGN KEY ("sector_key") REFERENCES "dim_sector" ("sector_key");

ALTER TABLE "fact_marketwatch" ADD FOREIGN KEY ("shariah_key") REFERENCES "dim_shariahcompliance" ("shariah_key");

ALTER TABLE "fact_marketwatch" ADD FOREIGN KEY ("loaddate_key") REFERENCES "dim_date" ("date_key");

