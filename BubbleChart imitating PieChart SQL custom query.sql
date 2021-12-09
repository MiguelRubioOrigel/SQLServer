WITH
cte_data AS (
--This can be changed to any query using any datasource from an actual client table from which we can derive the components of a percentage metric
SELECT 0 AS Color, 32 AS Slice
UNION
SELECT 2 AS Color, 6 AS Slice
UNION
SELECT 3 AS Color, 25 AS Slice
),
cte_radii AS (
SELECT CAST(1 AS FLOAT) AS radius
UNION ALL
SELECT radius + CAST(1 AS FLOAT)
FROM cte_radii
WHERE radius <= 9
),
cte_grados_delta AS (
SELECT radius + 1 AS JoinBase, 360 / CAST(11 * radius AS FLOAT) AS GradosDelta FROM cte_radii WHERE radius<10
),
base1 AS (
SELECT CAST(1 AS FLOAT) AS radius, CAST(0 AS FLOAT) AS Grados
UNION ALL
SELECT radius + CAST(1 AS FLOAT) AS radius, CAST(0 AS FLOAT)
FROM base1
WHERE radius <= 9
),
base2 AS (
SELECT  a.radius, a.Grados, ISNULL(b.GradosDelta,0) AS GradosDelta
FROM base1 a
LEFT JOIN cte_grados_delta b
	ON a.radius = b.JoinBase
),
cte_5 AS (
SELECT radius, grados FROM base2 WHERE radius = 5
UNION ALL
SELECT a.radius, a.grados + b.GradosDelta
FROM cte_5 a 
INNER JOIN base2 b
	ON a.radius = b.radius
WHERE a.grados < 360
),
cte_6 AS (
SELECT radius, grados FROM base2 WHERE radius = 6
UNION ALL
SELECT a.radius, a.grados + b.GradosDelta
FROM cte_6 a 
INNER JOIN base2 b
	ON a.radius = b.radius
WHERE a.grados < 360
),
cte_7 AS (
SELECT radius, grados FROM base2 WHERE radius = 7
UNION ALL
SELECT a.radius, a.grados + b.GradosDelta
FROM cte_7 a 
INNER JOIN base2 b
	ON a.radius = b.radius
WHERE a.grados < 360
),
cte_8 AS (
SELECT radius, grados FROM base2 WHERE radius = 8
UNION ALL
SELECT a.radius, a.grados + b.GradosDelta
FROM cte_8 a 
INNER JOIN base2 b
	ON a.radius = b.radius
WHERE a.grados < 360
),
cte_9 AS (
SELECT radius, grados FROM base2 WHERE radius = 9
UNION ALL
SELECT a.radius, a.grados + b.GradosDelta
FROM cte_9 a 
INNER JOIN base2 b
	ON a.radius = b.radius
WHERE a.grados < 360
),
cte_10 AS (
SELECT radius, grados FROM base2 WHERE radius = 10
UNION ALL
SELECT a.radius, a.grados + b.GradosDelta
FROM cte_10 a 
INNER JOIN base2 b
	ON a.radius = b.radius
WHERE a.grados < 360
),
cte_full_circle AS (
SELECT radius,grados FROM cte_5
UNION ALL
SELECT radius,grados FROM cte_6
UNION ALL
SELECT radius,grados FROM cte_7
UNION ALL
SELECT radius,grados FROM cte_8
UNION ALL
SELECT radius,grados FROM cte_9
UNION ALL
SELECT radius,grados FROM cte_10
),
cte_color_base AS (
SELECT	 color AS Color_Code
	,CASE WHEN color = 0 THEN 'Gray' WHEN color = 1 THEN 'Red' WHEN color = 2 THEN 'Yellow' WHEN color = 3 THEN 'Green' END AS Color
	,slice / CAST((SELECT SUM(slice) FROM cte_data) AS FLOAT) * 360 AS Grados
FROM cte_data
),
cte_color_limits AS (
SELECT	 Color
	,(SELECT ISNULL(SUM(Grados),0) FROM cte_color_base b where b.Color_Code < a.Color_Code) AS StartSlice
	,(SELECT ISNULL(SUM(Grados),0) FROM cte_color_base b where b.Color_Code < a.Color_Code) + Grados AS EndSlice
FROM cte_color_base a
)

SELECT	 radius
	,grados
	,x = radius * cos(grados * pi() / 180)
	,y = radius * sin(grados * pi() / 180)
	,limits.Color
--INTO #ctedemo
FROM cte_full_circle circle
INNER JOIN cte_color_limits limits
	ON circle.grados BETWEEN limits.StartSlice AND limits.EndSlice
WHERE grados <= 360