from informatics.daris.system import (
    DarisInformaticsSystem, DarisProject, DarisExperiment,
    DarisScan)
import os.path
import subprocess
import contextlib
from lxml import etree
from nipype.interfaces.base import (
    Directory, DynamicTraitedSpec, traits, TraitedSpec)
from nipype.interfaces.io import IOBase
from mbi_pipelines.exception import DarisException
import zipfile


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


class DarisSourceInputSpec(TraitedSpec):
    project_id = traits.Int(mandatory=True, desc='The project ID')  # @UndefinedVariable @IgnorePep8
    subject_ids = traits.Enum(None, traits.List(  # @UndefinedVariable
        traits.Int(mandatory=True, desc="id of subjects")))  # @UndefinedVariable @IgnorePep8
    experiment_id = traits.Int(1, mandatory=True, usedefult=True,  # @UndefinedVariable @IgnorePep8
                               desc="The experiment ID")
    mode_id = traits.Int(1, mandatory=True, usedefault=True,  # @UndefinedVariable @IgnorePep8
                         desc=("The mode of the dataset (Parnesh is using 1 "
                               "for data and 2 for processed data"))
    repo_id = traits.Int(2, mandatory=True, usedefault=True, # @UndefinedVariable @IgnorePep8
                         desc='The ID of the repository')
    scan_names = traits.List(  # @UndefinedVariable
        traits.Str(mandatory=True, desc="name of scan"),  # @UndefinedVariable
        desc="Names of all scans that comprise the dataset")
    cache_dir = Directory(
        exists=True, desc=("Path to the base directory where the downloaded"
                           "scans will be cached"))
    server = traits.Str('mf-erc.its.monash.edu.au', mandatory=True,  # @UndefinedVariable @IgnorePep8
                        usedefault=True, desc="The address of the MF server")
    domain = traits.Str('monash-ldap', mandatory=True, usedefault=True,  # @UndefinedVariable @IgnorePep8
                        desc="The domain of the username/password")
    user = traits.Str(None, mandatory=True, usedefault=True,  # @UndefinedVariable @IgnorePep8
                      desc="The DaRIS username to log in with")
    password = traits.Password(None, mandatory=True, usedefault=True,  # @UndefinedVariable @IgnorePep8
                               desc="The password of the DaRIS user")


class DarisSource(IOBase):
    input_spec = DarisSourceInputSpec
    output_spec = DynamicTraitedSpec

    def _list_outputs(self):
        with create_session(server=self.inputs.server,
                            domain=self.inputs.domain,
                            user=self.inputs.user,
                            password=self.inputs.password) as session:
            outputs = {}
            for scan_name in self.inputs.scan_names:
                outputs[scan_name] = []
                if self.inputs.subject_ids is None:
                    raise NotImplementedError  # Need to download from Daris
                else:
                    subject_ids = self.inputs.subject_ids
                for subject_id in subject_ids:
                    # FIXME: Get scan ids from names
                    scan_id = 1
                    cid = "1008.{}.{}.{}.{}.{}.{}".format(
                        self.inputs.repo_id, self.inputs.project_id,
                        self.inputs.subject_id, self.inputs.mode_id,
                        self.inputs.experiment_id, scan_id)
                    cache_path = os.path.join(
                        self.inputs.cache_dir, self.inputs.project_id,
                        subject_id, self.inputs.experiment_id,
                        self.inputs.mode_id)
                    if not os.path.exists(cache_path):
                        # Download zip file
                        session.run(
                            "asset.get :cid {cid} :out file: {path}.zip"
                            .format(cid=cid, path=cache_path))
                        # Unzip zip file
                        with zipfile.ZipFile("zipfile.zip", "r") as zf:
                            assert len(zf.infolist()) == 1
                            member = next(iter(zf.infolist()))
                            zf.extract(member, cache_path)
                        # Remove zip file
                        os.remove(cache_path + '.zip')
                outputs[scan_name].append(cache_path)
        return outputs


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


class DarisSession:
    """
    Handles the connection to the MediaFlux server, logs into the DaRIS
    application and runs MediaFlux commands

    NB: The function 'create_session' is the preferred way to create a
    DarisSession rather than calling hte DarisSession __init__ method directly
    (unless in an interactive session) as it ensures the session is logged off
    when it is no longer required
    """
    daris_ns = 'daris'

    def __init__(self, server='mf-erc.its.monash.edu.au', domain='monash-ldap',
                 user=None, password=None):
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
            if isinstance(xpath, basestring):
                result = self._extract_from_xml(result, xpath)
                if single:
                    try:
                        result = result[0].text
                    except IndexError:
                        raise DarisException(
                            "No results found for '{}' xpath".format(xpath))
            else:
                # If 'xpath' is a iterable of xpaths then extract each in turn
                result = [self._extract_from_xml(result, p) for p in xpath]
        return result

    def close(self):
        self.run('logoff')
        self._open = False

    @classmethod
    def _extract_from_xml(cls, xml_string, xpath):
        doc = etree.XML(xml_string.strip())
        return doc.xpath(xpath, namespace=cls.daris_ns)

    @classmethod
    def aterm_path(cls):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'jar', 'aterm.jar')
