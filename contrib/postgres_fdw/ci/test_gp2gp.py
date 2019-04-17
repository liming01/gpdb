#!/usr/bin/python2

import optparse
import os
import shutil
import stat
import subprocess
import sys

def install_gpdb(dependency_name):
    status = subprocess.call("mkdir -p /usr/local/gpdb", shell=True)
    if status:
        return status
    status = subprocess.call(
        "tar -xzf " + dependency_name + "/*.tar.gz -C /usr/local/gpdb",
        shell=True)
    return status


def create_demo_cluster():
    return subprocess.call([
        "runuser gpadmin -c \"source {0}/greenplum_path.sh \
        && make create-demo-cluster DEFAULT_QD_MAX_CONNECT=150\"".format("/usr/local/gpdb")],
        cwd="gpdb_src/gpAux/gpdemo", shell=True)


def create_gpadmin_user():
    status = subprocess.call("gpdb_src/concourse/scripts/setup_gpadmin_user.bash")
    os.chmod('/bin/ping', os.stat('/bin/ping').st_mode | stat.S_ISUID)
    if status:
        return status

def run_gp2gp_tests():
    status = create_demo_cluster()
    if status:
        return status
    status = subprocess.call("runuser gpadmin -c \"source /usr/local/gpdb/greenplum_path.sh \
            && source gpAux/gpdemo/gpdemo-env.sh && PGOPTIONS='-c optimizer=off' \
                make installcheck-gp2gp -C src/test/isolation2\"", cwd="gpdb_src", shell=True)
    if status:
        return status
    status = subprocess.call("runuser gpadmin -c \"source /usr/local/gpdb/greenplum_path.sh \
            && source gpAux/gpdemo/gpdemo-env.sh && PGOPTIONS='-c optimizer=off' \
                make installcheck -C contrib/postgres_fdw\"", cwd="gpdb_src", shell=True)
    if status:
        return status


def copy_output():
    for dirpath, dirs, diff_files in os.walk('gpdb_src/'):
        if 'regression.diffs' in diff_files:
            diff_file = dirpath + '/' + 'regression.diffs'
            print(  "======================================================================\n" +
                    "DIFF FILE: " + diff_file+"\n" +
                    "----------------------------------------------------------------------")
            with open(diff_file, 'r') as fin:
                print fin.read()


def configure():
    p_env = os.environ.copy()
    p_env['LD_LIBRARY_PATH'] = '/usr/local/gpdb/lib'
    p_env['CFLAGS'] = '-I/usr/local/gpdb/include'
    p_env['CPPFLAGS'] = '-I/usr/local/gpdb/include'
    p_env['LDFLAGS'] = '-L/usr/local/gpdb/lib'
    return subprocess.call(["./configure",
                            "--enable-mapreduce",
                            "--enable-orafce",
                            "--with-gssapi",
                            "--with-perl",
                            "--with-libxml",
                            "--with-python",
                            # TODO: remove this flag after zstd is vendored in the installer for ubuntu
                            "--without-zstd",
                            "--with-libs=/usr/local/gpdb/lib",
                            "--with-includes=/usr/local/gpdb/include",
                            "--prefix=/usr/local/gpdb"], env=p_env, cwd="gpdb_src")


def main():
    parser = optparse.OptionParser()
    parser.add_option("--build_type", dest="build_type", default="RELEASE")
    parser.add_option("--mode",  choices=['orca', 'planner'])
    parser.add_option("--compiler", dest="compiler")
    parser.add_option("--cxxflags", dest="cxxflags")
    parser.add_option("--output_dir", dest="output_dir", default="install")
    parser.add_option("--gpdb_name", dest="gpdb_name")
    (options, args) = parser.parse_args()

    status = install_gpdb(options.gpdb_name)
    if status:
        return status
    status = configure()
    if status:
        return status
    status = create_gpadmin_user()
    if status:
        return status
    status = run_gp2gp_tests()
    if status:
        copy_output()
    return status


if __name__ == "__main__":
    sys.exit(main())
