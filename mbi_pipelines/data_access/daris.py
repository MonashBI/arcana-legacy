import os.path
import subprocess
import zipfile
import stat
from lxml import etree
from nipype.interfaces.base import (
    Directory, DynamicTraitedSpec, traits, TraitedSpec)
from nipype.interfaces.io import IOBase
from mbi_pipelines.exception import DarisException


class DarisSession:
    """
    Handles the connection to the MediaFlux server, logs into the DaRIS
    application and runs MediaFlux commands
    """
    _namespaces = {'daris': 'daris'}
    DEFAULT_REPO = 2

    def __init__(self, server='mf-erc.its.monash.edu.au', domain='monash-ldap',
                 user=None, password=None, token_path=None,
                 app_name='python_daris'):
        """
        server     -- the host name or IP of the daris server
        domain     -- the login domain of the user to login with
        user       -- the username of the user to login with
        password   -- the password for the user
        token_path -- path to the token file to use for authentication. If it
                      doesn't exist it will be created using the username and
                      password provided
        """
        if user is None:
            user = os.environ.get('DARIS_USER', None)
        if password is None:
            password = os.environ.get('DARIS_PASSWORD', None)
        if ((token_path is None or not os.path.exists(token_path)) and
                None in (user, password)):
            raise DarisException(
                "Username and password must be provided if no token is "
                "given and the environment variables 'DARIS_USER' and "
                "'DARIS_PASSWORD' are not set")
        if token_path is None:
            self._token = None
        elif os.path.exists(token_path):
            with open(token_path) as f:
                self._token = f.readline()
        else:
            # Generate token if it doesn't already exist
            self._token = self.run(
                "secure.identity.token.create :app {}".format(app_name))
            # ":destroy-on-service-call system.logoff"
            with open(token_path, 'w') as f:
                f.write(self._token)
            # Change permissions to owner read only
            os.chmod(token_path, stat.S_IRUSR)
        self._server = server
        self._domain = domain
        self._user = user
        self._password = password
        self._app_name = app_name
        self._mfsid = None
        self._logging_on = False

    def open(self):
        """
        Opens the session. Should usually be used within a 'with' context, e.g.

            with DarisSession() as session:
                session.run("my-cmd")

        to ensure that the session is always closed afterwards
        """
        if self._token is not None:
            # Get MediaFlux SID from token logon
            self._mfsid = self.run("system.logon :app {} :token {}"
                                    .format(self._app_name, self._token),
                                    logon=True)
        else:
            # Logon to DaRIS using user name
            self._mfsid = self.run("logon {} {} {}".format(
                self._domain, self._user, self._password), logon=True)

    def close(self):
        if self._mfsid:
            self.run('logoff')
            self._mfsid = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, type_, value, traceback):  # @UnusedVariable
        self.close()

    def download(self, location, project_id, subject_id, scan_id,
                 time_point=1, processed=False, repo_id=2):
        # Construct CID
        cid = "1008.{}.{}.{}.{}.{}.{}".format(
            repo_id, project_id, subject_id, (processed + 1), time_point,
            scan_id)
        # Download zip file
        self.run("asset.get :cid {cid} :out file: __{path}__.zip"
                    .format(cid=cid, path=location))
        # Unzip the zip file
        with zipfile.ZipFile(
                "__{}__.zip".format(location), "r") as zf:
            assert len(zf.infolist()) == 1
            member = next(iter(zf.infolist()))
            zf.extract(member, location.strip())
        # Clean up zip file
        os.remove("__{}__.zip".format(location))

    def upload(self, location, repo_id, project_id, subject_id, mode_id,
               experiment_id, scan_id):
        raise NotImplementedError

    def list_projects(self, attrs=('name', 'description'), repo_id=2):
        """
        Lists all projects in the repository

        repo_id     -- the ID of the DaRIS repo (Monash is 2)
        attrs       -- iterable of attributes to return from the query along
                       with the ID. Valid options are: 'name', 'description',
                       'ctime' and 'mtime'
        """
        results = self.query(
            "cid starts with '1008.{}' and model='om.pssd.project'"
            .format(repo_id),
            xpaths=['cid'] + self._prepend_subset(
                attrs, 'meta/daris:pssd-object/', ('name', 'description')))
        # Return the requested attributes extracting the project ID from the
        # CID
        return [[r[0].split('.')[2]] + r[1:] for r in results]

    def list_subjects(self, project_id, attrs=('name', 'description'),
                      repo_id=2):
        """
        Lists all projects in a project

        project_id  -- the ID of the project to list the subjects for
        repo_id     -- the ID of the DaRIS repo (Monash is 2)
        attrs       -- iterable of attributes to return from the query along
                       with the ID. Valid options are: 'name', 'description',
                       'ctime' and 'mtime'
        """
        results = self.query(
            "cid starts with '1008.{}.{}' and model='om.pssd.subject'"
            .format(repo_id, project_id),
            xpaths=['cid'] + self._prepend_subset(
                attrs, 'meta/daris:pssd-object/', ('name', 'description')))
        # Return the requested attributes extracting the project ID from the
        # CID
        return [[r[0].split('.')[3]] + r[1:] for r in results]

    def list_studies(self, project_id, subject_id, repo_id=2, processed=False,
                     attrs=('name', 'description')):
        results = self.query(
            "cid starts with '1008.{}.{}.{}.{}' and model='om.pssd.study'"
            .format(repo_id, project_id, subject_id, (processed + 1)),
            xpaths=['cid'] + self._prepend_subset(
                attrs, 'meta/daris:pssd-object/', ('name', 'description')))
        # Return the requested attributes extracting the project ID from the
        # CID
        return [[r[0].split('.')[5]] + r[1:] for r in results]

    def list_scans(self, project_id, subject_id, study_id=1, repo_id=2,
                   processed=False, attrs=('name', 'description')):
        results = self.query(
            "cid starts with '1008.{}.{}.{}.{}.{}' and model='om.pssd.dataset'"
            .format(repo_id, project_id, subject_id, (processed + 1),
                    study_id),
            xpaths=['cid'] + self._prepend_subset(
                attrs, 'meta/daris:pssd-object/', ('name', 'description')))
        # Return the requested attributes extracting the project ID from the
        # CID
        return [[r[0].split('.')[6]] + r[1:] for r in results]

    def print_list(self, lst):
        for id_, name, descr in lst:
            print '{} {}: {}'.format(id_, name, descr)

    def run(self, cmd, xpath=None, expect_single=False, logon=False):
        """
        Executes the aterm.jar and runs the provided aterm command within it

        cmd    -- The aterm command to run
        xpath  -- An xpath filter to the desired element(s)
        single -- Whether the filtered elements should only contain a single
                  result, and if so return its text field instead of the
                  etree.Element
        """
        if self._mfsid is None and not logon:
            raise DarisException(
                "Daris session is closed. DarisSessions are typically used "
                "within 'with' blocks, which ensures they are opened and "
                "closed properly")
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
        # Extract results from result XML if xpath is provided
        if xpath is not None:
            if isinstance(xpath, basestring):
                result = self._extract_from_xml(result, xpath)
                if expect_single:
                    try:
                        result = result[0].text
                    except IndexError:
                        raise DarisException(
                            "No results found for '{}' xpath".format(xpath))
            else:
                # If 'xpath' is a iterable of xpaths then extract each in turn
                result = [self._extract_from_xml(result, p) for p in xpath]
        return result

    def query(self, query, xpaths):
        """
        Runs a query command and returns the elements corresponding to the
        provided xpaths
        """
        print query
        cmd = ("asset.query :where \"{}\" :action get-meta :size infinity"
               .format(query))
        elements = self.run(cmd, '/result/asset')
        return [
            [e.xpath(xp, namespaces=self._namespaces)[0].text for xp in xpaths]
            for e in elements]

    @classmethod
    def _extract_from_xml(cls, xml_string, xpath):
        doc = etree.XML(xml_string.strip())
        return doc.xpath(xpath, namespaces=cls._namespaces)

    @classmethod
    def _prepend_subset(cls, names, prefix, to_prefix):
        """Prepends a prefix to the names found in 'to_prefix'"""
        return [prefix + n if n in to_prefix else n for n in names]

    @classmethod
    def aterm_path(cls):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'jar', 'aterm.jar')


class DarisSourceInputSpec(TraitedSpec):
    project_id = traits.Int(mandatory=True, desc='The project ID')  # @UndefinedVariable @IgnorePep8
    subject_id = traits.Int(mandatory=True, desc="The subject ID")  # @UndefinedVariable @IgnorePep8
    time_point = traits.Int(1, mandatory=True, usedefult=True,  # @UndefinedVariable @IgnorePep8
                            desc="The time point or processed data process ID")
    processed = traits.Bool(False, mandatory=True, usedefault=True,  # @UndefinedVariable @IgnorePep8
                            desc=("The mode of the dataset (Parnesh is using 1"
                                  " for data and 2 for processed data"))
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
        with DarisSession(server=self.inputs.server,
                          domain=self.inputs.domain,
                          user=self.inputs.user,
                          password=self.inputs.password) as daris:
            outputs = {}
            # Create dictionary mapping scan names to IDs
            scan_ids = dict(daris.list_scans(
                repo_id=self.inputs.repo_id,
                project_id=self.inputs.project_id,
                subject_id=self.inputs.subject_id,
                processed=self.inputs.processed,
                time_point=self.inputs.time_point))
            for scan_name in self.inputs.scan_names:
                scan_id = scan_ids[scan_name]
                cache_path = os.path.join(
                    self.inputs.cache_dir, self.inputs.project_id,
                    self.inputs.subject_id, self.inputs.time_point,
                    self.inputs.mode_id, scan_id)
                if not os.path.exists(cache_path):
                    daris.download(
                        cache_path, repo_id=self.inputs.repo_id,
                        project_id=self.inputs.project_id,
                        subject_id=self.inputs.subject_id,
                        processed=self.inputs.processed,
                        time_point=self.inputs.time_point, scan_id=scan_id)
                outputs[scan_name] = cache_path
        return outputs

if __name__ == '__main__':
    with DarisSession(user='tclose', password='S8mmyD0g-') as daris:
        daris.print_list(daris.list_scans(88, 500, 1, processed=False))
