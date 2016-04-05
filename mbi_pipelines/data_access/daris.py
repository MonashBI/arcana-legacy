import os.path
import subprocess
import zipfile
import stat
from lxml import etree
from nipype.interfaces.base import (
    Directory, DynamicTraitedSpec, traits, TraitedSpec)
from nipype.interfaces.io import IOBase
from mbi_pipelines.exception import DarisException, DarisExistingCIDException


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

    def download(self, location, project_id, subject_id, dataset_id,
                      time_point=1, processed=False, repo_id=2):
        # Construct CID
        cid = "1008.{}.{}.{}.{}.{}.{}".format(
            repo_id, project_id, subject_id, (processed + 1), time_point,
            dataset_id)
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

    def upload(self, location, project_id, subject_id, study_id, dataset_id,
                    name=None, repo_id=2, processed=True):
        # Use the name of the file to be uploaded if the 'name' kwarg is
        # present
        if name is None:
            name = os.path.basename(location)
        # Determine whether file is NifTI depending on file extension
        file_format = (
            " :lctype nifti/series " if location.endswith('.nii.gz') else '')
        cmd = (
            "om.pssd.dataset.derivation.update :id 1008.{}.{}.{}.{} "
            " :in file: {} :filename \"{}\" {}".format(
                repo_id, project_id, subject_id, (processed + 1), study_id,
                dataset_id, name, location, file_format))
        return self.run(cmd, '/result/id/', expect_single=True)

    def delete(self, project_id, subject_id, study_id, dataset_id,
                    processed=True, repo_id=2):
        cmd = (
            "om.pssd.object.destroy :cid 1008.{}.{}.{}.{}.{} :destroy-cid true"
            .format(repo_id, project_id, subject_id, (processed + 1), study_id,
                    dataset_id))
        self.run(cmd)

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

    def list_datasets(self, project_id, subject_id, study_id=1, repo_id=2,
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

    def add_subject(self, project_id, subject_id=None, name='', description='',
                    repo_id=2):
        """
        Adds a new subject with the given subject_id within the given
        project_id.

        project_id  -- The id of the project to add the subject to
        subject_id  -- The subject_id of the subject to add. If not provided
                       the next available subject_id is used
        name        -- The name of the subject
        description -- A description of the subject
        """
        if subject_id is None:
            # Get the next unused subject id
            try:
                max_subject_id = max(int(s[0]) for s in self.list_subjects(
                    project_id, repo_id=repo_id, attrs=()))
            except ValueError:
                max_subject_id = 0  # If there are no subjects
            subject_id = max_subject_id + 1
        cmd = (
            "om.pssd.subject.create :data-use \"unspecified\" :description "
            "\"{}\" :method \"1008.1.16\" :name \"{}\" :pid 1008.{}.{} "
            ":subject-number {} ".format(
                description, name, repo_id, project_id, subject_id))
        return self.run(cmd, '/result/id', expect_single=True)

    def add_study(self, project_id, subject_id, study_id=None, name='',
                  description='', processed=True, repo_id=2):
        """
        Adds a new subject with the given subject_id within the given
        project_id

        project_id  -- The id of the project to add the study to
        subject_id  -- The id of the subject to add the study to
        study_id    -- The study_id of the study to add. If not provided
                       the next available study_id is used
        name        -- The name of the subject
        description -- A description of the subject
        """
        if study_id is None:
            # Get the next unused study id
            try:
                max_study_id = max(int(s[0]) for s in self.list_studies(
                    project_id, subject_id, processed=processed,
                    repo_id=repo_id, attrs=()))
            except ValueError:
                max_study_id = 0
            study_id = max_study_id + 1
        if processed:
            # Check to see whether processed "ex-method" (ex-method is being
            # co-opted to hold raw and processed data)
            sid = '1008.{}.{}.{}'.format(repo_id, project_id, subject_id)
            existing_processed = self.run(
                "asset.exists :cid {}.2".format(sid), '/result/exists',
                expect_single=True)
            # Create an "ex-method" to hold the processed data
            if existing_processed == 'false':
                self.run("om.pssd.ex-method.create :mid 1008.1.19 :sid {}"
                         " :exmethod-number 2".format(sid))
        cmd = (
            "om.pssd.study.create :pid 1008.{}.{}.{}.{} :processed {} "
            ":name \"{}\" :description \"{}\" :step 1 :study-number {}".format(
                repo_id, project_id, subject_id, (processed + 1),
                str(processed).lower(), name, description, study_id))
        return self.run(cmd, '/result/id')

    def add_dataset(self, project_id, subject_id, study_id, dataset_id,
                  name='', description='', processed=True, repo_id=2):
        """
        Adds a new dataset with the given subject_id within the given study id

        project_id  -- The id of the project to add the dataset to
        subject_id  -- The id of the subject to add the dataset to
        study_id    -- The id of the study to add the dataset to
        dataset_id     -- The dataset_id of the dataset to add. If not provided
                       the next available dataset_id is used
        name        -- The name of the subject
        description -- A description of the subject
        """
        if dataset_id is None:
            # Get the next unused dataset id
            try:
                max_dataset_id = max(int(s[0]) for s in self.list_datasets(
                    project_id, subject_id, study_id=study_id,
                    processed=processed, repo_id=repo_id, attrs=()))
            except ValueError:
                max_dataset_id = 0
            dataset_id = max_dataset_id + 1
        if processed:
            meta = ("' :step 1 :meta < :mbi.processed.study.properties < "
                    ":study-reference 1008.{}.{}.{}.1".format(
                        repo_id, project_id, subject_id))
        else:
            meta = ""
        cmd = ("om.pssd.dataset.derivation.create :pid 1008.{}.{}.{}.{}.{}"
               " :name \"{}\" :description \"{} :processed {}{}".format(
                   repo_id, project_id, subject_id, (processed + 1), study_id,
                   dataset_id, name, description, str(processed).lower(), meta))
        return self.run(cmd, '/result/id', expect_single=True)

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
        cmd = ("asset.query :where \"{}\" :action get-meta :size infinity"
               .format(query))
        elements = self.run(cmd, '/result/asset')
        results = []
        for element in elements:
            row = []
            results.append(row)
            for xpath in xpaths:
                extracted = element.xpath(xpath, namespaces=self._namespaces)
                if len(extracted) == 1:
                    attr = extracted[0].text
                elif not extracted:
                    attr = None
                else:
                    raise DarisException(
                        "Multiple results for given xpath '{}': {}"
                        .format(xpath, "', '".join(e.text for e in extracted)))
                row.append(attr)
        return results

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
    dataset_names = traits.List(  # @UndefinedVariable
        traits.Str(mandatory=True, desc="name of dataset"),  # @UndefinedVariable
        desc="Names of all sub-datasets that comprise the complete dataset")
    cache_dir = Directory(
        exists=True, desc=("Path to the base directory where the downloaded"
                           "datasets will be cached"))
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
            # Create dictionary mapping dataset names to IDs
            dataset_ids = dict(daris.list_datasets(
                repo_id=self.inputs.repo_id,
                project_id=self.inputs.project_id,
                subject_id=self.inputs.subject_id,
                processed=self.inputs.processed,
                time_point=self.inputs.time_point))
            for dataset_name in self.inputs.dataset_names:
                dataset_id = dataset_ids[dataset_name]
                cache_path = os.path.join(
                    self.inputs.cache_dir, self.inputs.project_id,
                    self.inputs.subject_id, self.inputs.time_point,
                    self.inputs.mode_id, dataset_id)
                if not os.path.exists(cache_path):
                    daris.download(
                        cache_path, repo_id=self.inputs.repo_id,
                        project_id=self.inputs.project_id,
                        subject_id=self.inputs.subject_id,
                        processed=self.inputs.processed,
                        time_point=self.inputs.time_point,
                        dataset_id=dataset_id)
                outputs[dataset_name] = cache_path
        return outputs

if __name__ == '__main__':
    with DarisSession(user='tclose', password='S8mmyD0g-') as daris:
        daris.add_study(4, 12, name='toms_test2_processed',
                        description='yet one more study testing new daris python module',
                        processed=True)
        daris.print_list(daris.list_studies(4, 12, processed=True))
