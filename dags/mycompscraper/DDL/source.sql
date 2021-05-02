create table ScrapComp (
	id serial primary key,
	CompanyFullName TEXT,
	StockShortName TEXT,
	StockCode TEXT,
	MarketName TEXT,
	Shariah TEXT,
	Sector TEXT,
	MarketCap TEXT,
	LastPrice TEXT,
	PriceEarningRatio TEXT,
	DividentYield text,
	ReturnOnEquity text,
	LoadDateTime timestamp
	)