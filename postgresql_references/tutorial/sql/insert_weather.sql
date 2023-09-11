/*
	
	POSTGRESQL TUTORIAL
	 - https://www.postgresql.org/docs/current/tutorial-populate.html
*/

-- INSERT STATEMENT
INSERT INTO weather (city, temp_lo, temp_hi, prcp, date)
     VALUES ('San Francisco', 43, 57, 0.0, '1994-11-29');

-- INSERT FROM FILE (must be absolute from root)
  COPY weather FROM '/home/nroshania/git/python-template/postgresql_references/tutorial/data/weather.txt' (DELIMITER ',');

