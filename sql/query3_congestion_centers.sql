-- ========================================================
-- PROJECT TITLE: Flight On-Time Performance Data Warehouse
-- STUDENT NAMES: Khaled, Fawzi, Bassam
-- STUDENT ID: 202310190, 23038671, 202220220
-- ========================================================

-- BUSINESS QUESTION: 
-- Which hubs serve as primary bottlenecks by exceeding the global average 
-- operational delay threshold, and what is their percentage contribution 
-- to system wide delays?

SELECT 
    f.origin AS HubAirport,
    COUNT(f.flight_id) AS TotalDepartures,
    ROUND(AVG(d.late_aircraft_delay), 2) AS HubAvgDelayMinutes,
    (
        SELECT ROUND(AVG(late_aircraft_delay), 2) 
        FROM flight_delays
    ) AS GlobalSystemAverageDelay,
    ROUND((SUM(d.late_aircraft_delay) / (
        SELECT SUM(late_aircraft_delay) 
        FROM flight_delays
    )) * 100, 2) AS PercentOfGlobalDelayWeight
FROM flights f
JOIN flight_delays d ON f.flight_id = d.flight_id
GROUP BY f.origin
HAVING HubAvgDelayMinutes > (SELECT AVG(late_aircraft_delay) FROM flight_delays)
ORDER BY HubAvgDelayMinutes DESC;