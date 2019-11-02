SELECT DISTINCT c_address
FROM customer, nation, region
WHERE c_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'AFRICA'


SELECT DISTINCT n_name,r_name
FROM nation, region
WHERE n_regionkey = r_regionkey AND r_name = 'AFRICA'


