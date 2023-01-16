SELECT primary_title,premiered,runtime_minutes,runtime_minutes||' (mins)'
FROM titles
ORDER BY runtime_minutes DESC
LIMIT 10;