CREATE TABLE IF NOT EXISTS public.numbeo_stat (
    geoname_id integer NOT NULL,
    param_id integer NOT NULL,
    cost numeric,
    range numrange,
    last_update date,
    updated_date date,
    updated_by varchar(30),
    CONSTRAINT pk_numbeo_stat PRIMARY KEY (geoname_id, param_id),
    CONSTRAINT fk_numbeo_stat_param_id
        FOREIGN KEY (param_id)
        REFERENCES public.numbeo_param (param_id)
);

CREATE INDEX IF NOT EXISTS idx_numbeo_stat_param_id
    ON public.numbeo_stat (param_id);
