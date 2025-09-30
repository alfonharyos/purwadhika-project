
  
    

    create or replace table `purwadika`.`jcdeol004_alfon_taxi_trip_model`.`dim_location`
      
    
    

    OPTIONS()
    as (
      


select
    locationid,
    borough,
    zone,
    service_zone
from `purwadika`.`jcdeol004_alfon_taxi_trip_raw`.`raw_taxi_zone`
    );
  