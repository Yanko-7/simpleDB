SELECT name,ifnull(2023,died)-born as AGE
FROM people
where born>=1900
ORDER BY AGE desc,name
LIMIT 20;
