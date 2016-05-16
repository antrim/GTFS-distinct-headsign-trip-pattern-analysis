// OK, some progress
// GOAL:
//	- Just find stops_patterns. What is the number of unique stop patterns?
//	- Can I group by stop_headsign_pattern and stop_pattern at the same time?

WITH timed_patterns AS (

WITH timed_patterns_sub AS (

WITH pattern_time_intervals AS(
SELECT MIN(trips.trip_id) as one_trip,string_agg( trips.trip_id::text, ', ' ORDER BY sequences.min_arrival_time ) AS trips_list, sequences.stops_pattern, headsign_pattern, arrival_time_intervals,departure_time_intervals,trips.agency_id,trips.route_id,trips.direction_id
FROM trips
INNER JOIN (

	 SELECT  string_agg(stop_times.stop_id::text , ', ' ORDER BY stop_times.stop_sequence ASC) AS stops_pattern, stop_times.trip_id, MIN( stop_times.arrival_time ) AS min_arrival_time

	 FROM stop_times
	 inner join trips on stop_times.trip_id = trips.trip_id
	 WHERE  trips.based_on IS NULL
	 GROUP BY stop_times.trip_id
	 ) AS sequences ON trips.trip_id = sequences.trip_id

INNER JOIN (

	 SELECT  string_agg(COALESCE(stop_times.headsign_id,0)::TEXT, ', ' ORDER BY stop_times.stop_sequence ASC) AS headsign_pattern, stop_times.trip_id, MIN( stop_times.arrival_time ) AS min_arrival_time

	 FROM stop_times
	 inner join trips on stop_times.trip_id = trips.trip_id
	 WHERE  trips.based_on IS NULL
	 GROUP BY stop_times.trip_id
	 ) AS headsign_orders ON trips.trip_id = headsign_orders.trip_id

INNER JOIN
	 (SELECT
	 min_arrival_time, min_departure_time, min_trip_times.trip_id, string_agg(
	 case when stop_times.arrival_time IS NOT NULL THEN (stop_times.arrival_time - min_arrival_time)::text ELSE ''
	   end
	  ,  ','  ORDER BY stop_times.stop_sequence ASC) as arrival_time_intervals,
	 string_agg(
	 case when stop_times.arrival_time IS NOT NULL THEN (stop_times.departure_time - min_departure_time)::text ELSE ''
	   end
	  ,  ','  ORDER BY stop_times.stop_sequence ASC) as departure_time_intervals 
	 FROM stop_times
		 inner join trips on stop_times.trip_id = trips.trip_id
		 INNER JOIN (
		 SELECT MIN( arrival_time ) AS min_arrival_time, MIN( departure_time ) AS min_departure_time,  stop_times.trip_id
		 FROM stop_times
		 inner join trips on stop_times.trip_id = trips.trip_id
		 WHERE  trips.based_on IS NULL
		 GROUP BY stop_times.trip_id
		 ) min_trip_times ON stop_times.trip_id = min_trip_times.trip_id
	 WHERE trips.based_on IS NULL
	 GROUP BY min_trip_times.trip_id,min_arrival_time,min_departure_time
	) AS time_intervals_result

ON sequences.trip_id = time_intervals_result.trip_id


WHERE  trips.based_on IS NULL
GROUP BY stops_pattern,headsign_pattern,arrival_time_intervals,departure_time_intervals,trips.agency_id,trips.route_id,trips.direction_id
)
SELECT pattern_time_intervals.* , MIN( stop_times.arrival_time ) AS min_arrival_time, MIN( stop_times.departure_time) AS min_departure_time
FROM pattern_time_intervals
inner join  stop_times on pattern_time_intervals.one_trip = stop_times.trip_id 
group by  one_trip,trips_list,stops_pattern, headsign_pattern,arrival_time_intervals,departure_time_intervals,pattern_time_intervals.agency_id,route_id,direction_id

) select row_number() over() as timed_pattern_id, * from timed_patterns_sub ),

stop_patterns AS (


WITH unique_patterns AS(
SELECT DISTINCT string_agg(stop_times.stop_id::text , ', ' ORDER BY stop_times.stop_sequence ASC) AS stops_pattern,trips.route_id,trips.direction_id

	 FROM stop_times
	 inner join trips on stop_times.trip_id = trips.trip_id
	 WHERE  trips.based_on IS NULL
	 GROUP BY stop_times.trip_id,trips.route_id,trips.direction_id)
SELECT unique_patterns.stops_pattern,route_id,direction_id,row_number() over() as pattern_id from unique_patterns

)

SELECT string_agg(one_trip::TEXT, ',') as list_trips,timed_patterns.agency_id,timed_patterns.stops_pattern,agency.agency_name,routes.route_short_name,routes.route_long_name,directions.direction_label,trips.direction_id, COUNT (DISTINCT headsigns.headsign_id::TEXT || headsigns.headsign || headsign_pattern) AS headsign_combo_count,
string_agg(DISTINCT headsigns.headsign::TEXT, ',') as list_trip_headsigns FROM timed_patterns
inner JOIN stop_patterns ON (timed_patterns.stops_pattern = stop_patterns.stops_pattern AND timed_patterns.route_id = stop_patterns.route_id AND timed_patterns.direction_id = stop_patterns.direction_id)
inner join trips on timed_patterns.one_trip = trips.trip_id
inner join routes on trips.route_id = routes.route_id
left join directions on trips.direction_id = directions.direction_id
left join headsigns on trips.headsign_id = headsigns.headsign_id
inner join agency on trips.agency_id = agency.agency_id
GROUP BY timed_patterns.agency_id,timed_patterns.stops_pattern,agency.agency_name,routes.route_short_name,routes.route_long_name,directions.direction_label,trips.direction_id,stop_patterns.pattern_id
ORDER BY headsign_combo_count DESC,agency_name