CREATE TABLE mig_test.supplier (
    s_suppkey integer NOT NULL ENCODE raw,
    s_name character varying(25) NOT NULL ENCODE lzo,
    s_address character varying(25) NOT NULL ENCODE lzo,
    s_city character varying(10) NOT NULL ENCODE lzo,
    s_nation character varying(15) NOT NULL ENCODE lzo,
    s_region character varying(12) NOT NULL ENCODE lzo,
    s_phone character varying(15) NOT NULL ENCODE lzo
)
DISTSTYLE ALL
SORTKEY ( s_suppkey );