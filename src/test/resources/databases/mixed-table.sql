CREATE TABLE "TestTable"
(
   "RecordId" INTEGER PRIMARY KEY NOT NULL,
   "Name" VARCHAR(255) NOT NULL,
   "IsNew" BOOLEAN NOT NULL,
   "Size" DOUBLE NOT NULL,
   "Price" DECIMAL NOT NULL,
   "Volume" BIGINT NOT NULL,
   "SomeTimestamp" TIMESTAMP NOT NULL,
   "SomeDateTime" DATETIME NOT NULL,
   "SomeDate" DATE NOT NULL,
   "CharData" CHAR(2) NOT NULL,
   "TinyIntData" TINYINT NOT NULL,
   "SmallIntData" SMALLINT NOT NULL,
   "RealData" REAL NOT NULL
);



insert into "TestTable" ("RecordId", "Name", "IsNew", "Size", "Price", "Volume", "SomeTimestamp", "SomeDateTime", "SomeDate", "CharData", "TinyIntData", "SmallIntData", "RealData")
	values (1, 'S&P 500 Spider', 1, 100, 102.34, 2340000, '2014-05-30 06:44:00.000', '2014-05-31 08:01:00.000', '2014-05-01', 'XX', 2, 3, 3.53556);

insert into "TestTable" ("RecordId", "Name", "IsNew", "Size", "Price", "Volume", "SomeTimestamp", "SomeDateTime", "SomeDate", "CharData", "TinyIntData", "SmallIntData", "RealData")
	values (2, 'Apple Computer', 0, 200, 756.34, 12340000, '2014-05-30 07:44:00.000', '2014-05-31 09:01:00.000', '2014-05-02', 'US', 3, 4, 345.53455);

insert into "TestTable" ("RecordId", "Name", "IsNew", "Size", "Price", "Volume", "SomeTimestamp", "SomeDateTime", "SomeDate", "CharData", "TinyIntData", "SmallIntData", "RealData")
	values (3, 'Microsoft', 0, 400, 23.45, 22340000, '2014-05-02 12:44:00.000', '2014-05-04 09:22:00.000', '2014-05-03', 'US', 4, 5, 0.3434534);
