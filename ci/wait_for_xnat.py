# Checks to see whether the XNAT host is up before continuing to run
# the unittests
import os
import xnat
import subprocess as sp
import time


EXPECTED_PREFIX = 'xnat-web_1    | INFO: Server startup in'
NUM_ATTEMPTS = 10
WAIT_TIME = 600

wait_incr = WAIT_TIME // NUM_ATTEMPTS

# Check the logs to see whether the server should be up
for i in range(NUM_ATTEMPTS):
    tail = sp.check_output(
        '/usr/local/bin/docker-compose logs -f --tail=1 xnat-web',
        shell=True)
    if tail.startswith(EXPECTED_PREFIX):
        break
    print("Found '{}' at tail of xnat-web log after {} seconds. "
          "Will wait up until {} seconds before attempting to "
          "connect".format(tail, i * wait_incr, WAIT_TIME))
    time.sleep(wait_incr)

# Test to see whether we can connect to the XNAT server
xnat.connect(os.environ['ARCANA_TEST_XNAT'], verify=True)
