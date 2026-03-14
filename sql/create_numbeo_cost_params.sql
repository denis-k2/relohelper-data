CREATE TABLE IF NOT EXISTS public.numbeo_cost_params (
    param_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_id integer NOT NULL,
    param varchar(255) NOT NULL,
    CONSTRAINT fk_numbeo_cost_params_category_id
        FOREIGN KEY (category_id)
        REFERENCES public.numbeo_cost_categories (category_id),
    CONSTRAINT uq_numbeo_cost_params_category_param
        UNIQUE (category_id, param)
);
