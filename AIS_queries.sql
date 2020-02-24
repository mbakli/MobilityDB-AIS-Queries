--Query 1: List the departure time of all ships in the port of Kalundborg between 2019-01-02 00:30 AM and 2019-01-02 01:00?
	SELECT T.ship_id, T.Trip, startTimestamp(atgeometry(T.trip, P.port_geom)) As DepartTime
	FROM ships T, ports P
	WHERE P.port_name='Kalundborg'
	AND T.trip && STBOX(P.port_geom, period('2019-01-02 00:30', '2019-01-02 01:00') )		
	and intersects(T.Trip, P.port_geom)
-------------------------------------------------------------------------------
-- Query 2: How many one way trips the ships did on September 2, 2019 between the ports of Rødby and Puttgarden?
	WITH TEMP
	AS 
	   (SELECT 
			(SELECT Port_geom AS Port_geom_Rodby from Ports where port_name='Rodby'), 
			(SELECT Port_geom AS Port_geom_Puttgarden from Ports where port_name='Puttgarden') 
	   )
	SELECT MMSI, (numSequences(atGeometry(S.Trip, P.Port_geom_Rodby)) +numSequences(atGeometry(S.Trip, P.Port_geom_Puttgarden)))/2.0 AS NumTrips
	FROM Ships S, TEMP P
	WHERE intersects(S.Trip, P.port_geom_Rodby) 
	AND intersects(S.Trip, P.port_geom_Puttgarden) 
-------------------------------------------------------------------------------
-- Query 3: What is the trajectory and speed of all ships that spent more than 5 days to reach to the Port of Kalundborg in September 2019?
	SELECT mmsi AS ShipID, trajectory(Trip) AS Traj, speed
	FROM Ships
	WHERE timespan(Trip) > '5 days'		
-------------------------------------------------------------------------------
-- Query 4:What is the length of all trips that ships did on September 2, 2019 to the ports of Puttgarden?
	SELECT mmsi ShipID, length(Trip)
	FROM Ships
	WHERE Destination='Puttgarden'
	AND Trip && Period('2019-09-02 00:00:00', '2019-09-02 23:59:59')
	INTO J;
-------------------------------------------------------------------------------
