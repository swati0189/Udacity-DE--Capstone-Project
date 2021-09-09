CREATE TABLE IF NOT EXISTS public.dim_listing (	
	listing_id int NOT NULL,
	date date NOT NULL,
	available char(1),
	price numeric(18,2),
	host_id int4,
    district_id INT,
    neighbourhood_id INT,
    CONSTRAINT dim_listing_pkey PRIMARY KEY (listing_id, date)
)
DISTKEY(listing_id);	
	
CREATE TABLE IF NOT EXISTS public.dim_host (	
	host_id int4 NOT NULL,
	host_name varchar(256),
	host_url varchar(5000),
	host_since date,
	host_location varchar(1000),
    host_has_profile_pic varchar(1),
    host_identity_verified varchar(1),
    room_type varchar(256),
    host_is_superhost varchar(1), 
	CONSTRAINT dim_host_pkey PRIMARY KEY (host_id)
)
DISTKEY(host_id);	
	
CREATE TABLE IF NOT EXISTS public.dim_district (	
	district_id int IDENTITY(1,1),
	district varchar(256) NOT NULL,
	CONSTRAINT dim_district_pkey PRIMARY KEY (district_id)
)
DISTKEY(district_id);	

CREATE TABLE IF NOT EXISTS public.dim_review (
		listing_id INT
        ,review_id INT
        ,review_scores_rating DECIMAL(5,2)
        ,number_of_reviews INT
        ,review_scores_accuracy DECIMAL(5,2)
        ,review_scores_cleanliness DECIMAL(5,2) 
        ,review_scores_communication DECIMAL(5,2)
        ,review_scores_location	 DECIMAL(5,1)
        ,review_scores_value DECIMAL(5,1)
 )
 DISTKEY(review_id);
	
CREATE TABLE IF NOT EXISTS public.dim_neighbourhood (	
	neighbourhood_id int IDENTITY(1,1),
    district_id int4,	
	neighbourhood varchar(256) NOT NULL,
	CONSTRAINT dim_neighbourhood_pkey PRIMARY KEY (neighbourhood_id)
)
DISTKEY(neighbourhood_id);	
	
CREATE TABLE IF NOT EXISTS public.dim_property (	
	listing_id int4 NOT NULL,
	host_id int4 NOT NULL,
	property_type varchar(2000),
	room_type varchar(256),
	accommodates int4,
	bathrooms varchar(256),
	bedrooms int4,
	beds int4,	
	amenities varchar(5000),	
	latitude numeric(18,0),	
	longitude numeric(18,0),	
    CONSTRAINT dim_property_pkey PRIMARY KEY (listing_id, host_id)
)
DISTKEY(listing_id);	
	
CREATE TABLE IF NOT EXISTS dim_date (	
	id_date  INTEGER PRIMARY KEY,	
	date_actual DATE,	
	day_name TEXT,	
	day_of_week INTEGER,	
	day_of_month INTEGER,	
    current_date_flag INTEGER,	
    week_of_month INTEGER,	
    week_of_year INTEGER,	
    month_number INTEGER,	
    month_name TEXT,	
    quarter_actual INTEGER,		
    year_actual INTEGER,	
    weekend_ind TEXT	
)
DISTKEY(id_date);		
	
CREATE TABLE IF NOT EXISTS public.staging_listing (	
	id varchar(5000),
	listing_url varchar(5000),
    scrape_id varchar(5000),	
    last_scraped varchar(5000),	
    name  varchar(5000),	
    description  varchar(5000),	
    neighborhood_overview  varchar(5000),	
    picture_url  varchar(5000),
    host_id varchar(5000),	
    host_url  varchar(5000),	
    host_name  varchar(5000),	
    host_since varchar(5000),	
    host_location  varchar(5000),	
    host_about  varchar(25000),	
    host_response_time  varchar(5000),	
    host_response_rate  varchar(5000),	
    host_acceptance_rate  varchar(5000),	
    host_is_superhost varchar(5000),	
    host_thumbnail_url  varchar(5000),	
    host_picture_url  varchar(5000),	
    host_neighbourhood  varchar(5000),	
    host_listings_count varchar(5000),	
    host_total_listings_count varchar(5000),	
    host_verifications  varchar(5000),	
    host_has_profile_pic varchar(5000),	
    host_identity_verified varchar(5000),	
    neighbourhood  varchar(5000),	
    neighbourhood_cleansed  varchar(5000),	
    neighbourhood_group_cleansed  varchar(5000),	
    latitude varchar(5000),	
    longitude varchar(5000),	
    property_type  varchar(5000),	
    room_type  varchar(5000),	
    accommodates varchar(5000),	
    bathrooms  varchar(5000),	
    bathrooms_text  varchar(5000),	
    bedrooms varchar(5000),	
    beds varchar(5000),	
    amenities  varchar(5000),	
    price  varchar(5000),	
    minimum_nights varchar(5000),	
    maximum_nights varchar(5000),	
    minimum_minimum_nights varchar(5000),	
    maximum_minimum_nights varchar(5000),	
    minimum_maximum_nights varchar(5000),	
    maximum_maximum_nights varchar(5000),	
    minimum_nights_avg_ntm varchar(5000),	
    maximum_nights_avg_ntm varchar(5000),	
    calendar_updated varchar(5000),	
    has_availability varchar(5000),	
    availability_30 varchar(5000),	
    availability_60 varchar(5000),	
    availability_90 varchar(5000),	
    availability_365 varchar(5000),	
    calendar_last_scraped varchar(5000),	
    number_of_reviews varchar(5000),	
    number_of_reviews_ltm varchar(5000),	
    number_of_reviews_l30d varchar(5000),	
    first_review varchar(5000),	
    last_review varchar(5000),	
    review_scores_rating varchar(5000),	
    review_scores_accuracy varchar(5000),	
    review_scores_cleanliness varchar(5000),	
    review_scores_checkin varchar(5000),	
    review_scores_communication varchar(5000),	
    review_scores_location varchar(5000),	
    review_scores_value varchar(5000),	
    license varchar(5000),	
    instant_bookable varchar(5000),	
    calculated_host_listings_count varchar(5000),	
    calculated_host_listings_count_entire_homes varchar(5000),	
    calculated_host_listings_count_private_rooms varchar(5000),	
    calculated_host_listings_count_shared_rooms varchar(5000),	
    reviews_per_month varchar(5000)	
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS public.staging_calendar (	
	listing_id varchar,
	date varchar,
	available varchar,
	price varchar,
	adjusted_price varchar,
	minimum_nights varchar,
	maximum_nights varchar
)
DISTSTYLE EVEN;

CREATE TABLE IF NOT EXISTS public.staging_review (
	 listing_id	varchar,
     review_id varchar,
     review_date varchar,
     reviewer_id	varchar,
     reviewer_name	varchar(5000),
     comments varchar(50000)
 )
DISTSTYLE EVEN;

CREATE TABLE IF NOT EXISTS public.fact_neighbourhood (	
	neighborhood_id INT,
	date DATE,
	total_number_of_listings INT,
	number_of_available_listing INT,
    number_of_unavailable_listing INT,
	min_price NUMERIC(10,2),
	max_price NUMERIC(10,2),
	avg_price NUMERIC(10,2)
)
DISTSTYLE EVEN;

CREATE TABLE IF NOT EXISTS public.fact_listing (	
	listing_id INT
	,neighbourhood_id INTEGER 
	,avg_rating DECIMAL(10,2) 
    ,review_count INT
	,is_premium CHAR(1) 
)
DISTSTYLE EVEN;


Truncate table staging_calendar;

Truncate table staging_listing;

Truncate table dim_date;

truncate table dim_neighbourhood;

truncate table dim_district;

truncate table dim_listing;

truncate table dim_property;

truncate table fact_neighbourhood;

truncate table dim_host;

truncate table fact_listing;

truncate table staging_review;

truncate table dim_review;