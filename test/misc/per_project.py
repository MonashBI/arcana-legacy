from nianalysis.archive.xnat import XNATSource

source = XNATSource()

source.inputs.server = 'https://mbi-xnat.erc.monash.edu.au'
source.inputs.user = 'tclose'
source.inputs.password = 'Saecha8y'
source.inputs.cache_dir = '/Users/tclose/Desktop/xnat-cache'
source.inputs.project_id = 'MMH004'
source.inputs.subject_id = 'MMH004_001'
source.inputs.visit_id = 'MRPT01'
source.inputs.datasets = [
    ('rsfPET_training_set', 'dicom', 'per_project', False, False)]
source.inputs.fields = []

source.run()
