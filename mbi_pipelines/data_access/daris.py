from informatics.daris.system import (
    DarisInformaticsSystem, DarisProject, DarisExperiment,
    DarisScan)
import os.path


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
