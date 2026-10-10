-- ========================================================
-- PROJECT TITLE: Flight On-Time Performance Data Warehouse
-- STUDENT NAMES: Khaled, Fawzi, Bassam
-- STUDENT ID: 202310190, 23038671, 202220220
-- ========================================================

-- BUSINESS QUESTION: 
-- How does a carrier's average aircraft delay on any given day compare 
-- to their rolling baseline across the operational timeline?

WITH DailyCarrierMetrics AS (
    SELECT 
        f.carrier_code AS Carrier,
        f.fl_date AS FlightDate,
        COUNT(f.flight_id) AS DailyFlightVolume,
        AVG(d.late_aircraft_delay) AS DailyAvgDelay
    FROM flights f
    JOIN flight_delays d ON f.flight_id = d.flight_id
    GROUP BY f.carrier_code, f.fl_date
)
SELECT 
    Carrier,
    FlightDate,
    DailyFlightVolume,
    ROUND(DailyAvgDelay, 2) AS DailyAvgDelayMinutes,
    ROUND(AVG(DailyAvgDelay) OVER (
        PARTITION BY Carrier 
        ORDER BY FlightDate 
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ), 2) AS Rolling7DayAvgDelay
FROM DailyCarrierMetrics
ORDER BY Carrier, FlightDate;