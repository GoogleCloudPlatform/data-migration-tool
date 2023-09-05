CREATE TABLE mig_test.customer (
    c_custkey bigint NOT NULL ENCODE raw distkey,
    c_name character varying(25) NOT NULL ENCODE lzo,
    c_address character varying(40) NOT NULL ENCODE lzo,
    c_nationkey integer NOT NULL ENCODE az64,
    c_phone character(15) NOT NULL ENCODE lzo,
    c_acctbal numeric(12,2) NOT NULL ENCODE az64,
    c_mktsegment character(10) NOT NULL ENCODE lzo,
    c_comment character varying(117) NOT NULL ENCODE lzo,
    PRIMARY KEY (c_custkey)
)
DISTSTYLE KEY
SORTKEY ( c_custkey );