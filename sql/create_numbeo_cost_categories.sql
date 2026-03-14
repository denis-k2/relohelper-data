CREATE TABLE IF NOT EXISTS public.numbeo_cost_categories (
    category_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category varchar(100) NOT NULL UNIQUE
);
