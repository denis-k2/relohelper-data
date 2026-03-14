INSERT INTO numbeo_param (category_id, param)
SELECT c.category_id, v.param
FROM numbeo_category c
CROSS JOIN (
    VALUES
        ('The estimated monthly costs for a family of four'),
        ('The estimated monthly costs for a single person')
) AS v(param)
WHERE c.category = 'Summary'
ON CONFLICT (category_id, param) DO NOTHING;
