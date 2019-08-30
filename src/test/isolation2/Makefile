#
# Makefile for isolation2 tests
#

subdir = src/test/isolation2
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

NAME = isolation2_regress
OBJS = isolation2_regress.o
include $(top_srcdir)/src/Makefile.shlib

# where to find psql for testing an existing installation
PSQLDIR = $(bindir)

ifeq ($(PORTNAME), win32)
LDLIBS += -lws2_32
endif

override CPPFLAGS := -I$(srcdir) -I$(libpq_srcdir) -I$(srcdir)/../regress $(CPPFLAGS)
override LDLIBS := $(libpq_pgport) $(LDLIBS)

all: test_python pg_isolation2_regress$(X) all-lib

test_python:
	python helpers_test.py

pg_regress.o:
	$(MAKE) -C $(top_builddir)/src/test/regress pg_regress.o
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/pg_regress.o .

gpstringsubs.pl:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/gpstringsubs.pl

gpdiff.pl: atmsort.pm explain.pm GPTest.pm
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/gpdiff.pl

GPTest.pm:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/GPTest.pm

atmsort.pm:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/atmsort.pm

explain.pm:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/explain.pm

pg_isolation2_regress$(X): isolation2_main.o pg_regress.o submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $(filter %.o,$^) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@

clean distclean:
	rm -f pg_isolation2_regress$(X) $(OBJS) isolation2_main.o
	rm -f pg_regress.o
	rm -f gpstringsubs.pl gpdiff.pl atmsort.pm explain.pm
	rm -rf $(pg_regress_clean_files)

install: all gpdiff.pl gpstringsubs.pl

installcheck: install
	./pg_isolation2_regress $(EXTRA_REGRESS_OPTS) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_isolation2 --psqldir='$(PSQLDIR)' --inputdir=$(srcdir) --ao-dir=uao --load-extension=gp_inject_fault --schedule=$(srcdir)/isolation2_schedule

installcheck-resgroup: install
	./pg_isolation2_regress $(EXTRA_REGRESS_OPTS) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_resgroup --psqldir='$(PSQLDIR)' --inputdir=$(srcdir) --dbname=isolation2resgrouptest --load-extension=gp_inject_fault --schedule=$(srcdir)/isolation2_resgroup_schedule

# PARALLEL RETRIEVE CURSOR related tests require 3 running segment. Some issues have been seen on CI that
# one segment is not running during the test. Add check here to make it fails earlier.
installcheck-parallel-retrieve-cursor-check-segments:
	gp_state=$$(gpstate) && \
	seg_count=$$(echo "$${gp_state}" | grep "Total primary segment valid (at master)" | awk '{print $$NF}') ; \
	if [ -z "$${seg_count}" ] || [ "$${seg_count}" != 3 ] ; then \
		echo "The tests require 3 valid segments." ; \
		echo "gpstate is: \n $${gp_state}" ; \
		echo "seg_count is: \n $${seg_count}" ; \
		exit -1 ; \
	fi

installcheck-parallel-retrieve-cursor: install installcheck-parallel-cursor-check-segments
	# TODO: Orca is not support by PARALLEL RETRIEVE CURSOR for now. Remove the optimizer
	# setting when it is supported.
	# before running test, set statement_timeout to 1 minute so that even if some
	# error make 'CHECK PARALLEL RETRIEVE CURSOR' hang, it will be canceled when timeout.
	default_optimizer=$$(gpconfig -s optimizer | grep "Master" | cut -d':' -f2) && \
	default_master_timeout=$$(gpconfig -s statement_timeout | grep "Master" | cut -d':' -f2) && \
	default_segment_timeout=$$(gpconfig -s statement_timeout | grep "Segment" | cut -d':' -f2) && \
	if [ -z "$${default_optimizer}" ] || [ -z "$${default_master_timeout}" ] || [ -z "$${default_segment_timeout}" ] ; then \
	  echo " \n --- default_optimizer is: $${default_optimizer} ---"; \
	  echo " --- default_master_timeout is: $${default_master_timeout} ---"; \
	  echo " --- default_segment_timeout is: $${default_segment_timeout} ---"; \
	  echo " Get gpconfig returns empty result " && exit -1; \
	fi; \
	gpconfig -c statement_timeout -v 60000 && \
	gpconfig -c optimizer -v false && \
	gpstop -u && \
	gpconfig -s statement_timeout && \
	./pg_isolation2_regress $(EXTRA_REGRESS_OPTS) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_parallel_retrieve_cursor --psqldir='$(PSQLDIR)' --inputdir=$(srcdir) --dbname=isolation2parallelretrcursor --schedule=$(srcdir)/parallel_retrieve_cursor_schedule ; \
	ret=$$? ; \
	gpconfig -c optimizer -v $${default_optimizer} -m $${default_optimizer} && \
	gpconfig -c statement_timeout -v $${default_segment_timeout} -m $${default_master_timeout} && \
	gpstop -u && \
	gpconfig -s optimizer && \
	gpconfig -s statement_timeout && \
	exit $${ret}
