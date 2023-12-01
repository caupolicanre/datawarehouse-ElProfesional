SELECT COUNT(*), fecha
FROM public.tiempo
GROUP BY fecha
HAVING COUNT(*) > 1