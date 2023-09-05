CREATE TABLE mig_test.part (
    p_partkey integer NOT NULL ENCODE raw distkey,
    p_name character varying(22) NOT NULL ENCODE lzo,
    p_mfgr character varying(6) NOT NULL ENCODE lzo,
    p_category character varying(7) NOT NULL ENCODE lzo,
    p_brand1 character varying(9) NOT NULL ENCODE lzo,
    p_color character varying(11) NOT NULL ENCODE lzo,
    p_type character varying(25) NOT NULL ENCODE lzo,
    p_size integer NOT NULL ENCODE az64,
    p_container character varying(10) NOT NULL ENCODE lzo
)
DISTSTYLE KEY
SORTKEY ( p_partkey );