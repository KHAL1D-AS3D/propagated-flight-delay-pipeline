-- ========================================================
-- PROJECT TITLE: Flight On-Time Performance Data Warehouse
-- STUDENT NAMES: Khaled, Fawzi, Bassam
-- STUDENT ID: 202310190, 23038671, 202220220
-- ========================================================

-- BUSINESS QUESTION: 
-- Which specific flight routes (Origin to Destination) experience the highest 
-- risk of severe delays, and how do they rank against each other within the 
-- same airline carrier?

WITH RouteDelayKPIs AS (
    SELECT 
        f.carrier_code AS Carrier,
        f.origin AS OriginAirport,
        f.destination AS DestinationAirport,
        COUNT(f.flight_id) AS TotalFlights,
        AVG(d.late_aircraft_delay) AS AvgAircraftDelay,
        SUM(f.cancelled) AS TotalCancellations
    FROM flights f
    JOIN flight_delays d ON f.flight_id = d.flight_id
    GROUP BY f.carrier_code, f.origin, f.destination
    HAVING TotalFlights > 50
)
SELECT 
    Carrier,
    OriginAirport,
    DestinationAirport,
    TotalFlights,
    ROUND(AvgAircraftDelay, 2) AS AvgAircraftDelayMinutes,
    TotalCancellations,
    DENSE_RANK() OVER (
        PARTITION BY Carrier 
        ORDER BY AvgAircraftDelay DESC
    ) AS DelayRankWithinCarrier
FROM RouteDelayKPIs
WHERE AvgAircraftDelay > 0
LIMIT 30;