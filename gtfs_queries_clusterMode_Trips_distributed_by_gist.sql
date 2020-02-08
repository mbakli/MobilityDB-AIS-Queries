/*DROP TABLE IF EXISTS execution_tests;
CREATE TABLE execution_tests (
	QuerySet int,
	Query char(5),
	Mode char(10),
	StartTime timestamp,
	PlanningTime float,
	ExecutionTime float,
	Duration interval,
	NumberRows bigint
);*/
---------------------------------------------------------
CREATE OR REPLACE FUNCTION gtfs_queries_clusterMode(detailed boolean default false) 
RETURNS text AS $$
DECLARE
	QuerySet int;
	Query char(5);
	Mode char(10);
	J json;
	StartTime timestamp;
	PlanningTime float;
	ExecutionTime float;
	Duration interval;
	NumberRows bigint;
BEGIN
--Distributed Tables: Trips_distributed_by_gist
	Mode = 'Cluster_trips_distributed_by_gist';
	SET log_error_verbosity to terse;
	
	--Query 1: List the time at which a vehicle visited a station '510 AZUL' between 9PM and 10PM on 2019-09-28?
	Query = 'Q1';
	FOR i in 1..10
	loop
		QuerySet = i;
		StartTime := clock_timestamp();

		-- Query 1: 11 ms.
		EXPLAIN (ANALYZE, FORMAT JSON)
		SELECT T.Route_id, T.Trip_id, MIN(startTimestamp(atValue(atPeriod(T.Trip, Period('2019-09-28 21:00', '2019-09-28 22:00')) ,S.Stop_geom))) AS Instant
		FROM Trips_distributed_by_gist T, Stops_cluster S
		WHERE S.stop_name = '510 AZUL'
		AND _intersects(atPeriod(T.Trip, Period('2019-09-28 21:00', '2019-09-28 22:00')), S.Stop_geom)
		GROUP BY T.Route_id, T.Trip_id
		INTO J;

		PlanningTime := (J->0->>'Planning Time')::float;
		ExecutionTime := (J->0->>'Execution Time')::float/1000;
		Duration := make_interval(secs := PlanningTime + ExecutionTime);
		NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
		IF detailed THEN
		RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
		trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
		ELSE
		RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
		END IF;
		INSERT INTO execution_tests VALUES (QuerySet, trim(Query), Mode, StartTime, PlanningTime, ExecutionTime, Duration, NumberRows);
	END loop;
	-------------------------------------------------------------------------------
	-- Query 2: What is the line name of the longest three trips?
	Query = 'Q2';
	FOR i in 1..10
	loop
		QuerySet = i;
		StartTime := clock_timestamp();

		EXPLAIN (ANALYZE, FORMAT JSON)
		With Trip_Dist AS
		(
		SELECT R.route_id, R.route_desc, sum(length(T.Trip)) as dist
		FROM Trips_distributed_by_gist T, routes_cluster R
		WHERE R.route_id = T.route_id
		GROUP BY R.route_id, R.route_desc
		)
		SELECT route_id, route_desc, max(dist) as MaxDist
		FROM Trip_Dist
		GROUP BY route_id, route_desc
		ORDER BY MaxDist desc
		limit 3
		INTO J;

		PlanningTime := (J->0->>'Planning Time')::float;
		ExecutionTime := (J->0->>'Execution Time')::float/1000;
		Duration := make_interval(secs := PlanningTime + ExecutionTime);
		NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
		IF detailed THEN
		RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
		trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
		ELSE
		RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
		END IF;
		INSERT INTO execution_tests VALUES (QuerySet, trim(Query), Mode, StartTime, PlanningTime, ExecutionTime, Duration, NumberRows);
	END loop;
	-------------------------------------------------------------------------------
	-- Query 3: What is the period that trips have been delayed more than 1 min for departing in the station of '20 DE SEPTIEMBRE 1787'?
	Query = 'Q3';
	FOR i in 1..10
	loop
		QuerySet = i;
		StartTime := clock_timestamp();

		EXPLAIN (ANALYZE, FORMAT JSON)
		SELECT timespan(atValue(T.Trip, S.Stop_geom))
		FROM Trips_distributed_by_gist T, Stops_cluster S, Routes_cluster R
		WHERE S.stop_name = '20 DE SEPTIEMBRE 1787'
		AND T.Route_id = R.Route_id
		AND _intersects(T.Trip, S.Stop_geom)
		AND (endtimestamp(atValue(T.Trip, S.Stop_geom)) - starttimestamp(atValue(T.Trip, S.Stop_geom))) >= '00:01:00'
		INTO J;

		PlanningTime := (J->0->>'Planning Time')::float;
		ExecutionTime := (J->0->>'Execution Time')::float/1000;
		Duration := make_interval(secs := PlanningTime + ExecutionTime);
		NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
		IF detailed THEN
		RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
		trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
		ELSE
		RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
		END IF;
		INSERT INTO execution_tests VALUES (QuerySet, trim(Query), Mode, StartTime, PlanningTime, ExecutionTime, Duration, NumberRows);
	END loop;
	-------------------------------------------------------------------------------
	-- Query 4:What are the 10 lines that have fewer stops in all agencies?
	Query = 'Q4';	
	FOR i in 1..10
	loop
		QuerySet = i;
		StartTime := clock_timestamp();

		EXPLAIN (ANALYZE, FORMAT JSON)
		SELECT A.Agency_name, R.route_id, R.route_desc, min(numinstants(T.Trip)) as minStops
		FROM Trips_distributed_by_gist T, Routes_cluster R, agency_cluster A
		WHERE T.Route_id = R.Route_id
		AND R.agency_id = A.agency_id
		GROUP BY A.Agency_name, R.Route_id, R.route_desc
		ORDER BY minStops
		LIMIT 10
		INTO J;

		PlanningTime := (J->0->>'Planning Time')::float;
		ExecutionTime := (J->0->>'Execution Time')::float/1000;
		Duration := make_interval(secs := PlanningTime + ExecutionTime);
		NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
		IF detailed THEN
		RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
		trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
		ELSE
		RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
		END IF;
		INSERT INTO execution_tests VALUES (QuerySet, trim(Query), Mode, StartTime, PlanningTime, ExecutionTime, Duration, NumberRows);
	END loop;
	-------------------------------------------------------------------------------
	-- Query 5:How many trips have passed through the commune of Agronomia for each agency?
	Query = 'Q5';
	FOR i in 1..10
	loop
		QuerySet = i;
		StartTime := clock_timestamp();

		EXPLAIN (ANALYZE, FORMAT JSON)
		SELECT count(T.trip_id), A.Agency_name
		FROM Trips_distributed_by_gist T, Routes_cluster R, agency_cluster A, Communes_cluster C
		WHERE C.name ='Agronomía'
		AND R.agency_id = A.agency_id
		AND T.route_id = R.route_id
		AND T.Trip && C.geom
		GROUP BY A.agency_name
		INTO J;

		PlanningTime := (J->0->>'Planning Time')::float;
		ExecutionTime := (J->0->>'Execution Time')::float/1000;
		Duration := make_interval(secs := PlanningTime + ExecutionTime);
		NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
		IF detailed THEN
		RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
		trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
		ELSE
		RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
		END IF;
		INSERT INTO execution_tests VALUES (QuerySet, trim(Query), Mode, StartTime, PlanningTime, ExecutionTime, Duration, NumberRows);
	END loop;
	-------------------------------------------------------------------------------
	-- Query 6:What are the lines that work on the weekend in September?
	Query = 'Q6';
	FOR i in 1..10
	loop
		QuerySet = i;
		StartTime := clock_timestamp();

		EXPLAIN (ANALYZE, FORMAT JSON)
		SELECT R.route_short_name
		FROM Trips_distributed_by_gist T, Routes_cluster R
		WHERE T.route_id = R.route_id
		AND EXTRACT(Month FROM starttimestamp(Trip)) = 9
		AND EXTRACT(ISODOW FROM  starttimestamp(Trip)) IN (6, 7)
		INTO J;

		PlanningTime := (J->0->>'Planning Time')::float;
		ExecutionTime := (J->0->>'Execution Time')::float/1000;
		Duration := make_interval(secs := PlanningTime + ExecutionTime);
		NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
		IF detailed THEN
		RAISE INFO 'Query: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Number of Rows: %', 
		trim(Query), StartTime, PlanningTime, ExecutionTime, Duration, NumberRows;
		ELSE
		RAISE INFO 'Query: %, Total Duration: %, Number of Rows: %', trim(Query), Duration, NumberRows;
		END IF;
		INSERT INTO execution_tests VALUES (QuerySet, trim(Query), Mode, StartTime, PlanningTime, ExecutionTime, Duration, NumberRows);
	END loop;

RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql';

SELECT gtfs_queries_clusterMode(TRUE);
--SELECT * from execution_tests where mode='Cluster';