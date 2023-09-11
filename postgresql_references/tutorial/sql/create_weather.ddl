/*
	
	POSTGRESQL TUTORIAL
	- https://www.postgresql.org/docs/current/tutorial-table.html
*/

-- DEFINE WEATHER
CREATE TABLE weather (
	city	varchar(80),
	temp_lo	int,  		-- low temperature
	temp_hi int, 		-- high temperature
	prcp	real, 		-- precipitation
	date	date
);
