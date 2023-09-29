SELECT p_partkey,
       p_mfgr,
       p_brand
FROM tpch.part
WHERE p_type like '%BRASS'