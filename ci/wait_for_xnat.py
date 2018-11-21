# Checks to see whether the XNAT host is up before continuing to run
# the unittests
import sys
import os.path
import xnat
import subprocess as sp
import time
import logging
import traceback

logger = logging.getLogger('xnat-test')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

NUM_RETRIES = 10
WAIT_TIME = 600

server = os.environ['ARCANA_TEST_XNAT']
xnat_path = os.environ['XNAT_DIR']

wait_incr = WAIT_TIME // NUM_RETRIES


# Keep attempting to connect to XNAT until number of retries has expired
for i in range(NUM_RETRIES + 1):
    try:
        # Test to see whether we can connect to the XNAT server
        xnat.connect(server, user='admin', password='admin', verify=True,
                     logger=logger, debug=True)
    except Exception:
        print("Could not access XNAT server '{}' after after {} "
              "seconds. Will wait up until {} seconds before "
              "giving up. ".format(server, i * wait_incr,
                                   WAIT_TIME))
        time.sleep(wait_incr)
        exc_type, exc_value, exc_tb = sys.exc_info()
        exception_str = ''.join(
            traceback.format_exception(exc_type, exc_value, exc_tb))
    else:
        print("Connected successfully to '{}'" .format(server))
        sys.exit(0)  # Exit with success
# Get original dir
orig_dir = os.getcwd()
try:
    # Change to docker compose directory to get xnat-web logs
    os.chdir(xnat_path)
    logs = sp.check_output(
        '/usr/local/bin/docker-compose logs xnat-web',
        shell=True)
    # Decode if using Python 3
    if (sys.version_info > (3, 0)):
        logs = str(logs.decode('utf-8'))
    with open(os.path.expanduser('~/.netrc')) as f:
        netrc = f.read()
    raise Exception(
        "Gave up attempting to access XNAT server '{}' after after {} "
        "seconds.\n\n"
        "The netrc file was:\n{}\n\n"
        "the XnatPy exception message was {}\n\n"
        "and the logs for xnat-web were:\n{}"
        .format(server, WAIT_TIME, netrc, exception_str, logs))
finally:
    os.chdir(orig_dir)
