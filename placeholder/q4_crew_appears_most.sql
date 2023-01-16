SELECT  p.name,COUNT(*) as cnt
FROM crew join people p on crew.person_id = p.person_id
GROUP BY crew.person_id
ORDER BY cnt desc
LIMIT 20;