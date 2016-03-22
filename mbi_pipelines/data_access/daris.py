from informatics.daris.system import (
    DarisInformaticsSystem, DarisProject, DarisExperiment,
    DarisScan)
import os.path
import subprocess
import contextlib
from lxml import etree
from mbi_pipelines.exception import DarisException


class DarisAccessor(object):

    MONASH_DARIS_HOSTNAME = 'mf-erc.its.monash.edu.au'
    MONASH_LDAP_DOMAIN = 'monash-ldap'
    ASPREE_DOWNLOAD_DIR_DEFAULT = '/data/reference/aspree'
    ASPREE_PROJECT_NUMBER = '1008.2.88'
    file_exts = {'nii': 'nii.gz'}  # FIXME: This is a bit of a hack

    def __init__(self, user, password,
                 aspree_download_dir=ASPREE_DOWNLOAD_DIR_DEFAULT):
        if not os.path.exists(aspree_download_dir):
            raise Exception("Provided aspree download directory '{}' does not "
                            "exist".format(aspree_download_dir))
        self._download_dir = aspree_download_dir
        self._user = user
        self._password = password
        self._daris = None

    def _connect_to_daris(self):
        if self._daris is None:
            self._daris = DarisInformaticsSystem()
            self._daris.initialise_creds(
                self.MONASH_DARIS_HOSTNAME,
                self.MONASH_LDAP_DOMAIN,
                self._user,
                self._password)
            self._daris.connect()
            self._aspree = DarisProject(
                self._daris.getAllProjects()[self.ASPREE_PROJECT_NUMBER].dPObj)
            # Load all subjects into self._aspree.subjects
            self._aspree.getAllSubjects()

    def get_scan(self, subject_id, scan_name, time_point=1, processed=False,
                 file_type='nii'):
        subject_dir = os.path.join(self._download_dir, subject_id)
        scan_path = os.path.join(
            subject_dir, scan_name + self.file_exts[file_type])
        # Check whether cached version of the scan exists else download it from
        # daris
        if not os.path.exists(scan_path):
            self._connect_to_daris()
            subject = self._aspree.subjects[
                '{}.{}'.format(self.ASPREE_PROJECT_NUMBER, subject_id)]
            # Construct experiment id
            experiment_id = (
                '{aspree}.{subject}.{raw_or_processed}.{time_point}'
                .format(aspree=self.ASPREE_PROJECT_NUMBER,
                        subject=subject_id,
                        raw_or_processed=(2 if processed else 1),
                        time_point=time_point))
            experiment = DarisExperiment(
                subject.getAllExperiments()[experiment_id].dEObj)
            scans = experiment.getAllScans()
            scan = DarisScan(next(s.dScObj for s in scans.itervalues()
                                  if s.dScObj['name'] == scan_name))
            # Download scan
            if not os.path.exists(subject_dir):
                os.mkdir(subject_dir)
            scan.downloadFile(file_type, subject_dir)
        return scan_path


@contextlib.contextmanager
def create_session(*args, **kwargs):
    """
    The safe way to connect to DaRIS so that the session is always logged off
    when it is no longer required
    """
    session = DarisSession(*args, **kwargs)
    try:
        yield session
    finally:
        session.close()


# NB: This is my attempt to clean up the DaRIS connection code, however it
#     hasn't been tested and is unused at this stage
class DarisSession:
    """
    Handles the connection to MediaFlux server and logs onto the DaRIS
    application
    """
    daris_ns = 'daris'

    def __init__(self, server='mf-erc.its.monash.edu.au', domain='monash-ldap',
                 user=None, password=None):
        """
        Creates the DarisSession. Note that the function 'create_session' is
        the preferred way to create a DarisSession as it ensures the session is
        logged off when it is no longer required
        """
        if user is None:
            try:
                user = os.environ['DARIS_USER']
            except KeyError:
                raise DarisException(
                    "No user provided and 'DARIS_USER' environment variable "
                    "not set")
        if password is None:
            try:
                password = os.environ['DARIS_PASSWORD']
                # TODO: Should use Francesco's ask_password at this point
            except KeyError:
                raise DarisException(
                    "No password provided and 'DARIS_PASSWORD' environment "
                    "variable not set")
        self._server = server
        self._mfsid = None  # Required so that it is ignored in the following
        # Logon to DaRIS using user name
        self._mfsid = self.run("logon {} {} {}".format(domain, user, password))
        # The following is used to log on using a pregenerated token
#         self._token = None
#         # Generate token (although not sure what a token is exactly,
#         # it is used for logging onto MediaFlux)
#         self._token = self.run(
#             "secure.identity.token.create :app {} "
#             ":destroy-on-service-call system.logoff".format(app))
#         # Get MediaFlux SID
#         self._mfsid = self.run("system.logon :app {} :token {}"
#                                 .format(app, self._token))
        self._open = True

    def __del__(self):
        try:
            if self._open:
                self.close()
        except AttributeError:
            pass  # Don't need to worry if the __init__ method didn't complete

    def run(self, cmd, xpath=None, single=False):
        """
        Executes the aterm.jar and runs the provided aterm command within it

        cmd    -- The aterm command to run
        xpath  -- An xpath filter to the desired element(s)
        single -- Whether the filtered elements should only contain a single
                  result, and if so return its text field instead of the
                  etree.Element
        """
        if self._mfsid is not None and not self._open:
            raise DarisException("Session has been closed")
        full_cmd = (
            "java -Djava.net.preferIPv4Stack=true -Dmf.host={server} "
            "-Dmf.port=8443 -Dmf.transport=https {mfsid}"
            "-Dmf.result=xml -cp {aterm_path} arc.mf.command.Execute {cmd}"
            .format(server=self._server, cmd=cmd, aterm_path=self.aterm_path(),
                    mfsid=('-Dmf.sid={} '.format(self._mfsid)
                           if self._mfsid is not None else '')))
        try:
            result = subprocess.check_output(
                full_cmd, stderr=subprocess.STDOUT, shell=True).strip()
        except subprocess.CalledProcessError, e:
            raise DarisException(
                "{}: {}".format(e.returncode, e.output.decode()))
        if xpath is not None:
            doc = etree.XML(result.strip())
            result = doc.xpath(xpath, namespace=self.daris_ns)
            if single:
                try:
                    result = result[0].text
                except IndexError:
                    raise DarisException(
                        "No results found for '{}' xpath".format(xpath))
        return result

    def close(self):
        self.run('logoff')
        self._open = False

    @classmethod
    def aterm_path(cls):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'jar', 'aterm.jar')
