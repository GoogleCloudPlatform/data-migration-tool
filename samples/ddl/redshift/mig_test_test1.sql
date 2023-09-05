CREATE TABLE mig_test.test1 (
    t_name character varying(25) NOT NULL ENCODE raw,
    t_number integer NOT NULL ENCODE az64,
    t_age character(10) NOT NULL ENCODE lzo
)
DISTSTYLE AUTO
SORTKEY ( t_name );