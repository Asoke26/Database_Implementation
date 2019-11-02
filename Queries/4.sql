SELECT DISTINCT c_address
FROM customer, nation, region
WHERE c_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'AFRICA'

SELECT SUM(n_nationkey), r_name
FROM nation, region
WHERE n_regionkey = r_regionkey AND r_regionkey=1 AND n_nationkey<10
GROUP BY r_name

SELECT count(c_address)
FROM customer



