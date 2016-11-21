'''
Created on 21Nov.,2016
Function to generate .csv file with all the volume measures extracted by
Freesurfer (recon-all). The order of the subjects is the one provided by the
Alfred hospital.
Parameters
----------

Inputs:
    daris_xls : excel file containing all the ASPREE NEURO ID (both ASPREE ID
                and DARIS ID).
    baseline_xls : excel file provided by the Alfred hospital.
    FS_out_dir : path to the directory with all the Freesurfer outputs
    out_dir : directory to save the .csv file
Outputs:
    excel file with all the stats extracted by Freesurfer recon-all
----------
@author: sforaz
'''
import pandas as pd
import csv


def gen_report(daris_xls, baseline_xls, FS_out_dir, out_dir):

    aspree_daris = pd.read_excel(daris_xls)
    aspree_baseline = pd.read_excel(baseline_xls)

    order = {}
    # I used 574 becouse there are 574 subjects in the ASPREE daris excel file.
    # check this number before running the script.
    for i in range(1, 574):
        order[str(int(aspree_daris['ASPREE ID'][i-1]))] = (
            'NEURO_'+str(i).zfill(3))

    mbi_id = {}
    for i in range(1, 574):
        try:
            mbi_id['NEURO_'+str(i).zfill(3)] = str(
                int(aspree_daris['SUBJECT ID MSH'][i-1]))
        except:
            print i
            continue

    not_prop = []
    list_sub = []
    for sub in aspree_baseline['Pt ID'][1:]:
        try:
            list_sub.append(order[str(int(sub))])
        except:
            not_prop.append(sub)

    all_sub_dicts = []
    no_stat_sub = []

    # lobes regions as defined in the freesurfer website
    # https://surfer.nmr.mgh.harvard.edu/fswiki/CorticalParcellation
    lobes = [['superiorparietal', 'inferiorparietal', 'supramarginal',
              'postcentral', 'precuneus'], ['lateraloccipital', 'lingual',
                                            'cuneus', 'pericalcarine'],
             ['superiorfrontal', 'rostralmiddlefrontal', 'caudalmiddlefrontal',
              'parsopercularis', 'parsorbitalis', 'parstriangularis',
              'lateralorbitofrontal', 'medialorbitofrontal', 'precentral',
              'paracentral', 'frontalpole'], ['superiortemporal',
                                              'middletemporal',
                                              'inferiortemporal', 'bankssts',
                                              'fusiform', 'transversetemporal',
                                              'entorhinal',
                                              'temporalpole',
                                              'parahippocampal']]

    for n_sub, sub in enumerate(list_sub):

        # dict_no_stat = {}
        whole_dict = {}

        try:
            open(FS_out_dir+sub+'/output_t1_t2/recon_all/stats/wmparc.stats')
            wmparc = (FS_out_dir+sub +
                      '/output_t1_t2/recon_all/stats/wmparc.stats')
            gmparc_l = (FS_out_dir+sub +
                        '/output_t1_t2/recon_all/stats/lh.aparc.stats')
            gmparc_r = (FS_out_dir+sub +
                        '/output_t1_t2/recon_all/stats/rh.aparc.stats')
            aseg = (FS_out_dir+sub +
                    '/output_t1_t2/recon_all/stats/aseg.stats')
        except:
            no_stat_sub.append(sub)
            whole_dict['Pt ID'] = ((aspree_baseline['Pt ID'][n_sub+1]))
            all_sub_dicts.append(whole_dict)
            # dict_no_stat['Pt ID']=((aspree_baseline['Pt ID'][n_sub+1]))
            # all_dict_no_stat.append(dict_no_stat)
            continue

        list_stat_file = [wmparc, gmparc_l, gmparc_r]

        lobe_names = ['PariLb', 'OcipLb', 'FrntLb', 'TempLb']

        # whole_dict={}
        for i, lobe in enumerate(lobes):

            for j, stat in enumerate(list_stat_file):
                list_stat = []
                width = 0

                with open(stat) as s:
                    for line in s:
                        l = line.split()
                        if len(l) > width:
                            width = len(l)
                        list_stat.append(l)

                left_vols = []
                right_vols = []
                for reg in lobe:

                    for line in list_stat:
                        if 'wm-lh-'+reg in line and j == 0:
                            left_vols.append(float(line[3]))
                        elif 'wm-rh-'+reg in line and j == 0:
                            right_vols.append(float(line[3]))
                        elif reg in line and j == 1:
                            left_vols.append(float(line[3]))
                        elif reg in line and j == 2:
                            right_vols.append(float(line[3]))

                right_tot_vol = sum(right_vols)
                left_tot_vol = sum(left_vols)
                if j == 0:
                    whole_dict[lobe_names[i]+'-L-WM'] = left_tot_vol
                    whole_dict[lobe_names[i]+'-R-WM'] = right_tot_vol
                elif j == 1:
                    whole_dict[lobe_names[i]+'-L-GM'] = left_tot_vol
                elif j == 2:
                    whole_dict[lobe_names[i]+'-R-GM'] = right_tot_vol

        list_stat = []
        width = 0

        with open(aseg) as s:
            for line in s:
                l = line.split()
                if len(l) > width:
                    width = len(l)
                list_stat.append(l)

        no_stat = ['18', '32']
        for line in list_stat:
            for i in range(1, 34):
                if line[0] == str(i) and line[0] not in no_stat:
                    whole_dict[line[4].replace('-', ' ')] = line[3]

        list_measure = ['Total Brain Vol', 'Brain Vol No Ventricles',
                        'Cortical GM', 'Sub-Cortical GM', 'Total GM Vol',
                        'Total WM Vol']

        measure = [13, 14, 18, 22, 23, 21]

        for i in range(len(measure)):
            whole_dict[list_measure[i]] = list_stat[measure[i]][-2].strip(',')

        whole_dict['Total CSF Vol'] = (
            float(whole_dict['Total Brain Vol']) -
            float(whole_dict['Brain Vol No Ventricles']))

        # bpf=(GM+WM)/(GM+WM+Ventricular CSF)
        bpf = (
            (float(whole_dict['Total GM Vol']) +
             float(whole_dict['Total WM Vol']) +
             float(whole_dict['Left Cerebellum White Matter']) +
             float(whole_dict['Right Cerebellum White Matter']))/(
                 float(whole_dict['Total GM Vol']) +
                 float(whole_dict['Total WM Vol']) +
                 float(whole_dict['Left Cerebellum White Matter']) +
                 float(whole_dict['Right Cerebellum White Matter']) +
                 float(whole_dict['Total CSF Vol'])-float(whole_dict['CSF'])))

        whole_dict['bpf'] = bpf
        try:
            whole_dict['Pt ID'] = (int(aspree_baseline['Pt ID'][n_sub+1]))
        except:
            whole_dict['Pt ID'] = ((aspree_baseline['Pt ID'][n_sub+1]))

        whole_dict['MRI File Name (MBI)'] = mbi_id[sub]
        all_sub_dicts.append(whole_dict)

    # all_sub_dicts_tot=all_sub_dicts+all_dict_no_stat
    ordered_keys = ['Pt ID', 'MRI File Name (MBI)',
                    'Total Brain Vol',
                    'Brain Vol No Ventricles',
                    'Total CSF Vol',
                    'Cortical GM',
                    'Sub-Cortical GM',
                    'Total GM Vol',
                    'Total WM Vol',
                    'bpf',
                    'FrntLb-L-WM',
                    'FrntLb-L-GM',
                    'FrntLb-R-WM',
                    'FrntLb-R-GM',
                    'TempLb-L-WM',
                    'TempLb-L-GM',
                    'TempLb-R-WM',
                    'TempLb-R-GM',
                    'PariLb-L-WM',
                    'PariLb-L-GM',
                    'PariLb-R-WM',
                    'PariLb-R-GM',
                    'OcipLb-L-WM',
                    'OcipLb-L-GM',
                    'OcipLb-R-WM',
                    'OcipLb-R-GM',
                    'Left Cerebellum Cortex',
                    'Right Cerebellum Cortex',
                    'Left Cerebellum White Matter',
                    'Right Cerebellum White Matter', 'Brain Stem',
                    'Left Lateral Ventricle', 'Right Lateral Ventricle',
                    'Left Inf Lat Vent', 'Right Inf Lat Vent',
                    '3rd Ventricle',
                    '4th Ventricle',
                    '5th Ventricle',
                    'Left vessel', 'Right vessel',
                    'Left VentralDC', 'Right VentralDC',
                    'CSF',
                    'Left Caudate', 'Right Caudate',
                    'Left Putamen', 'Right Putamen',
                    'Left Thalamus Proper', 'Right Thalamus Proper',
                    'Left Hippocampus', 'Right Hippocampus',
                    'Left Pallidum', 'Right Pallidum',
                    'Left Amygdala', 'Right Amygdala',
                    'Left Accumbens area', 'Right Accumbens area']

    print no_stat_sub
    print not_prop
    # list_of_dict=[whole_dict,whole_dict]
    fieldnames = ordered_keys
    with open(out_dir+'ASPREE_baseline_Freesurfer_stats.csv', 'w') as out_file:
        writer = csv.DictWriter(out_file, fieldnames=fieldnames,
                                dialect='excel')
        writer.writeheader()
        writer.writerows(all_sub_dicts)

if __name__ == "__main__":

    daris_xls = (
        '/Users/sforaz/Downloads/ASPREE Neuro Daris Numbers_modified_FS.xlsx')
    baseline_xls = (
        '/Users/sforaz/Downloads/ASPREE-NEURO Baseline MRI analyses.xlsx')
    FS_out_dir = ('/Users/sforaz/Desktop/T2_ASPREE_IMAGES/')
    out_dir = '/Users/sforaz/Desktop/'

    gen_report(daris_xls, baseline_xls, FS_out_dir, out_dir)
