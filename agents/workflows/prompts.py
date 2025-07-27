from datetime import datetime, timedelta


def clustering_prompt(time_delta_in_days=1):
    current_time = datetime.now()
    start_time = current_time - timedelta(days=time_delta_in_days)
    return f"""
    Can you execute the following query using `data_science_agent` and return output in its raw form
    
    SELECT
    c.name_of_cluster,
    c.central_latitude AS cluster_center_latitude,
    c.central_longitude AS cluster_center_longitude,
    c.radius_km AS cluster_radius_km,
    ARRAY_AGG(
    STRUCT(
    lp.id AS livepulse_id,
    lp.record_time,
    lp.location AS livepulse_location,
    lp.sub_location,
    lp.category,
    lp.sub_category,
    lp.source,
    lp.ai_analysis,
    lp.department,
    lp.severity,
    lp.latitude AS livepulse_latitude,
    lp.longitude AS livepulse_longitude,
    
        ST_DISTANCE(
            ST_GEOGPOINT(lp.longitude, lp.latitude), 
            ST_GEOGPOINT(c.central_longitude, c.central_latitude)
        ) AS distance_from_cluster_center_meters
    )
    ORDER BY lp.record_time DESC 
    ) AS livepulse_records
    FROM
    schrodingers-cat-466413.vectraCityRaw.BengaluruClusters AS c
    INNER JOIN
    schrodingers-cat-466413.vectraCityRaw.LivePulse AS lp
    ON
    
    lp.latitude IS NOT NULL AND lp.longitude IS NOT NULL
    
    AND ST_DISTANCE(
    ST_GEOGPOINT(lp.longitude, lp.latitude),
    ST_GEOGPOINT(c.central_longitude, c.central_latitude)
    ) <= (c.radius_km * 1000)
    AND lp.record_time >= {start_time} AND lp.record_time <= {current_time}
    GROUP BY
    c.name_of_cluster,
    c.central_latitude,
    c.central_longitude,
    c.radius_km
    ORDER BY
    c.name_of_cluster
    """


def insights_prompt():
    return """
    The data received in the previous step are the clusters with events with their category, sub category and summary. For each cluster can you create a list of emerging patterns, potential risks, and provide early warning systems based on clustered data analysis. With each also link a confidence score
   """
