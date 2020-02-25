--Query 1: List the departure time of all ships in the port of Kalundborg between 2019-01-02 00:30 and 01:00 AM.
	SELECT T.ship_id, T.Trip, startTimestamp(atgeometry(T.trip, P.port_geom)) As DepartTime
	FROM ships T, ports P
	WHERE P.port_name='Kalundborg'
	AND T.trip && STBOX(P.port_geom, period('2019-01-02 00:30', '2019-01-02 01:00') )		
	and intersects(T.Trip, P.port_geom);
-------------------------------------------------------------------------------
-- Query 2: How many one-way trips that ships did on September 2, 2019 between the ports of Rødby and Puttgarden?
	WITH TEMP
	AS 
	   (SELECT 
			(SELECT Port_geom AS Port_geom_Rodby from Ports where port_name='Rodby'), 
			(SELECT Port_geom AS Port_geom_Puttgarden from Ports where port_name='Puttgarden') 
	   )
	SELECT Ship_id, (numSequences(atGeometry(S.Trip, P.Port_geom_Rodby)) +numSequences(atGeometry(S.Trip, P.Port_geom_Puttgarden)))/2.0 AS NumTrips
	FROM Ships S, TEMP P
	WHERE intersects(S.Trip, P.port_geom_Rodby) 
	AND intersects(S.Trip, P.port_geom_Puttgarden); 
-------------------------------------------------------------------------------
-- Query 3: What is the trajectory and speed of all ships that spent more than 5 days to reach to the port of Kalundborg in Sept 19?
	SELECT Ship_id, trajectory(Trip) AS Traj, speed(Trip) AS TripSpeed
	FROM Ships
	WHERE Destination='Kalundborg'
	AND Trip && Period('2019-09-01', '2019-09-30')
	AND timespan(Trip) > '5 days';
-------------------------------------------------------------------------------
--Query 4: List the minimum distance ever between each ship and a specific point
	SELECT Ship_id, MIN(trajectory(Trip) <-> ST_transform(ST_setsrid(ST_MakePoint(10.945873, 56.447255), 4326), 25832)) AS MinDistance
	FROM Ships_dist 
	GROUP BY mmsi
	ORDER BY mmsi;

-------------------------------------------------------------------------------
-- Query 5: What is the total travelled distance of all trips between the ports of Rødby and Puttgarden in Sept 2, 19?
	SELECT Ship_id, length(Trip)
	FROM Ships
	WHERE Destination='Puttgarden'
	AND Trip && Period('2019-09-02 00:00:00', '2019-09-02 23:59:59');

-------------------------------------------------------------------------------
--Query 6: What are the ships that have passed a specific area in the sea?
	SELECT Ship_id
	FROM Ships
	WHERE intersects(Trip, ST_MakeEnvelope(640730, 6058230, 654100, 6042487, 25832));
-------------------------------------------------------------------------------
