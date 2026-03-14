CREATE TABLE IF NOT EXISTS public.numbeo_city_costs (
    geoname_id integer NOT NULL,
    param_id integer NOT NULL,
    cost numeric,
    range numrange,
    last_update date,
    updated_date date,
    updated_by varchar(30),
    CONSTRAINT pk_numbeo_city_costs PRIMARY KEY (geoname_id, param_id),
    CONSTRAINT fk_numbeo_city_costs_param_id
        FOREIGN KEY (param_id)
        REFERENCES public.numbeo_cost_params (param_id)
);
