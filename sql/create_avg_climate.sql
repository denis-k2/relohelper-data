CREATE TABLE IF NOT EXISTS avg_climate (
    city_id integer NOT NULL,
    geonameid integer,
    month smallint NOT NULL,
    {climate_params}
    updated_date date,
    updated_by varchar(30),
    CONSTRAINT pk_avg_climate PRIMARY KEY (city_id, month),
    CONSTRAINT fk_city_id FOREIGN KEY (city_id) REFERENCES public.cities (city_id)
);