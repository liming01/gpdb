--
-- Test foreign-data wrapper and server management. Greenplum MPP specific
--

-- start_ignore
DROP FOREIGN DATA WRAPPER dummy CASCADE;
-- end_ignore

CREATE FOREIGN DATA WRAPPER dummy;
COMMENT ON FOREIGN DATA WRAPPER dummy IS 'useless';

-- CREATE FOREIGN SERVER
CREATE SERVER s0 FOREIGN DATA WRAPPER dummy OPTIONS (mpp_size '0'); --ERROR

CREATE SERVER s0 FOREIGN DATA WRAPPER dummy;

CREATE SERVER s1 FOREIGN DATA WRAPPER dummy OPTIONS (mpp_execute 'all segments', mpp_size '6');

-- CREATE FOREIGN TABLE
CREATE FOREIGN TABLE ft2 (
	c1 int
) SERVER s0 OPTIONS (delimiter ',', mpp_execute 'a');           -- ERROR
CREATE FOREIGN TABLE ft2 (
	c1 int
) SERVER s0 OPTIONS (delimiter ',', mpp_execute 'any');
\d+ ft2
CREATE FOREIGN TABLE ft3 (
	c1 int
) SERVER s0 OPTIONS (delimiter ',', mpp_execute 'master');
CREATE FOREIGN TABLE ft4 (
	c1 int
) SERVER s0 OPTIONS (delimiter ',', mpp_execute 'all segments');

CREATE FOREIGN TABLE ft5 (
	c1 int
) SERVER s1 OPTIONS (mpp_size '-1');           -- ERROR
CREATE FOREIGN TABLE ft6 (
	c1 int
) SERVER s1 OPTIONS (mpp_size '8');
\d+ ft6
CREATE FOREIGN TABLE ft7 (
	c1 int
) SERVER s1;
\d+ ft7
