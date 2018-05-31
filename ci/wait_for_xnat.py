# Checks to see whether the XNAT host is up before continuing to run
# the unittests
import sys
import os.path
import xnat
import subprocess as sp
import time

NUM_ATTEMPTS = 10
WAIT_TIME = 600

server = os.environ['ARCANA_TEST_XNAT']
dc_path = os.path.expanduser('~/packages/xnat-docker-compose')

wait_incr = WAIT_TIME // NUM_ATTEMPTS

orig_dir = os.getcwd()


try:
    os.chdir(dc_path)
    for i in range(NUM_ATTEMPTS):
        try:
            # Test to see whether we can connect to the XNAT server
            xnat.connect(server, verify=True)
            print("Connected successfully to '{}'" .format(server))
            sys.exit(0)  # Exit with success
        except ValueError:
            print("Could not access XNAT server '{}' after after {} "
                  "seconds. Will wait up until {} seconds before "
                  "giving up. ".format(server, i * wait_incr,
                                       WAIT_TIME))
            time.sleep(wait_incr)
    logs = sp.check_output(
        '/usr/local/bin/docker-compose logs xnat-web',
        shell=True)
    with open(os.path.expanduser('~/.netrc')) as f:
        netrc = f.read()
    raise Exception(
        "Gave up attempting to access XNAT server '{}' after after {} "
        "seconds. The netrc file was:\n{}\n\nand the logs for xnat-web "
        "were:\n{}".format(server, WAIT_TIME, netrc, logs))
finally:
    os.chdir(orig_dir)
