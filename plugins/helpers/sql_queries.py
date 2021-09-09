class SqlQueries:
    fact_neighbourhood_insert = ("""
        SELECT neighbourhood_id,
        date,
        count(listing_id) as total_number_of_listings,
        count( case when  lower(available) = 't'  then 1  else NULL end ) as number_of_available_listing,
        count( case when  lower(available) = 'f'  then 1  else NULL end ) as number_of_un_available_listing,
        min(replace(replace(price,'$',''),',','')::numeric) as min_price,
        max(replace(replace(price,'$',''),',','')::numeric ) as max_price,
        avg(replace(replace(price,'$',''),',','')::numeric ) as  avg_price
        FROM dim_listing
        group by neighbourhood_id, date
    """)
        
    fact_listing_table_insert = ("""
         SELECT distinct dl.listing_id
        ,dl.neighbourhood_id
        ,review_scores_rating :: DECIMAL(10,2) AS avg_rating
        ,number_of_reviews:: INTEGER AS review_count
        ,CASE WHEN lower(host_has_profile_pic)	= 't' and
        lower(host_identity_verified) = 't' and
        room_type =	'Entire Home/apt' and
        lower(host_is_superhost) =	't' and review_scores_rating	> 4.5 and
        review_scores_accuracy	>4.5 and
        review_scores_cleanliness	> 4.5 AND 
        review_scores_communication	> 4.5 and
        review_scores_location	> 4.5 and
        review_scores_value	> 4.5   THEN 'Y' ELSE 'N' END IS_PREMIUM
        FROM dim_listing dl 
        join dim_host dh 
        on dl.host_id =dh.host_id 
        join dim_review dr 
        on dl.listing_id = dr.listing_id  
    """)
    
    dim_listing_table_insert = ("""
        SELECT distinct
        c.listing_id::INTEGER,
        c.date::DATE, 
        c.available::CHAR(1),
        replace(replace(c.price,'$',''),',','')::numeric as price,
        l.host_id::INTEGER,
        d.district_id,
        n.neighbourhood_id
        FROM staging_calendar c
        JOIN staging_listing l
        ON c.listing_id::INTEGER = l.id::INTEGER
        JOIN dim_district d
        ON l.neighbourhood_group_cleansed = d.district
        JOIN dim_neighbourhood n
        ON l.neighbourhood_cleansed = n.neighbourhood
        where replace(replace(c.price,'$',''),',','')::numeric != 0
    """)

    dim_host_table_insert = ("""
        SELECT distinct host_id :: INTEGER,
        host_name,
        host_url,
        to_date(host_since, 'YYYYMMDD') :: DATE as host_since,
        host_location,
        host_has_profile_pic ,
        host_identity_verified,
        room_type,
        host_is_superhost 
        from staging_listing
    """)
    
    dim_review_table_insert = ("""
        SELECT  l.id :: INT
        ,review_id ::  INT
        ,review_scores_rating :: DECIMAL(5,2)
        ,number_of_reviews :: INT
        ,review_scores_accuracy :: DECIMAL(5,2)
        ,review_scores_cleanliness :: DECIMAL(5,2) 
        ,review_scores_communication :: DECIMAL(5,2)
        ,review_scores_location	:: DECIMAL(5,1)
        ,review_scores_value :: DECIMAL(5,1)
        from staging_listing l 
        join staging_review r
        on l.id= r.listing_id
    """)
    
    dim_district_table_insert = ("""
        (district) SELECT distinct neighbourhood_group_cleansed as district
        FROM staging_listing 
    """)
    
    dim_neighbourhood_table_insert = ("""
        (district_id, neighbourhood) SELECT distinct 
d.district_id 
,l.neighbourhood_cleansed as neighbourhood
from dim_district d
left join staging_listing l
on d.district = l.neighbourhood_group_cleansed 
left join dim_neighbourhood tgt
on d.district = tgt.neighbourhood 
where tgt.district_id is NULL
     """)
    
    dim_property_table_insert = ("""
        SELECT id :: INTEGER as listing_id ,
        host_id :: INTEGER,
        property_type,
        room_type,
        accommodates:: INTEGER,
        bathrooms,
        bedrooms :: INTEGER,
        beds :: INTEGER,
        amenities,
        latitude :: numeric(18,0),
        longitude :: numeric(18,0)
        from staging_listing
    """)
    
    dim_date_table_insert = ("""
        with DQ as(
            with digit as (
                select 0 as d union all
                select 1 union all select 2 union all select 3 union all
                select 4 union all select 5 union all select 6 union all
                select 7 union all select 8 union all select 9
        ),
        seq as (
            select a.d + (10 * b.d) + (100 * c.d) + (1000 * d.d) as num
            from digit a
                cross join
                digit b
                cross join
                digit c
                cross join
                digit d
            order by 1
        )
        select (getdate()::date - seq.num)::date as "datum"
        from seq
        union
        select (getdate()::date+1 + seq.num)::date
        from seq
        )
        select distinct
        TO_CHAR(datum,'yyyymmdd')::INT AS id_date,
        datum AS date_actual,
        TO_CHAR(datum,'Day') AS day_name,
        CAST(DATE_PART(dow ,datum) AS INT) AS day_of_week,
        CAST(DATE_PART(DAY ,datum) AS INT) AS day_of_month,
        CASE WHEN DQ.datum = CURRENT_DATE THEN 1 ELSE 0 END AS current_date_flag,
        TO_CHAR(datum,'W')::INT AS week_of_month,
        CAST(DATE_PART(week ,datum) AS INTEGER) AS week_of_year,
        CAST(DATE_PART(MONTH ,datum) AS INTEGER) AS month_number,
        TO_CHAR(datum,'Month') AS month_name,
        CAST(DATE_PART(quarter ,datum) AS INTEGER) AS quarter_actual,
        CAST(DATE_PART(year ,datum) AS INTEGER) AS year_actual,
        CASE
            WHEN DATE_PART(dow ,datum) IN (6,0) THEN 'Y'
            ELSE 'N'
        END AS weekend_ind
        from DQ
    """)
        
        
