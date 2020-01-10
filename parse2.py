import re

def field_split(fields_string, separator=','):
    """
    splits a string using separator unless the separator is within parentheses
    takes string of fields

    returns list of fields
    """
    fields = []
    parenthesis_count = 0
    current_field = ''
    for char in fields_string:
        if char == ',' and parenthesis_count == 0: # split here by appending current field to fields and setting current_field to empty string
            fields.append(current_field)
            current_field = ''
        else:
            current_field = current_field + char
            if char == '(':
                parenthesis_count = parenthesis_count + 1
            elif char == ')':
                parenthesis_count = parenthesis_count - 1
    
    # add last field if it was not already added to fields
    if current_field != fields[-1]:
        fields.append(current_field)

    return fields
            
def parse_field(field):
    table_name, column_name, alias = '', '', ''
    field = field.lstrip()
    if field.upper().find(' AS ') == -1: # AS not found in string
        if field.find('.') > 0: # no AS, but contains dot (.) to separate table name and column name
            table_name, column_name = field.split('.')
        else: # no AS and no dot (only column name)
            column_name = field
    else:
        source = re.search('(.+?) AS ', field.upper(), re.DOTALL)
        #print("source: ", source.group(1))
        #print("---", source.group(1).upper()[0])
        if source:
            # check for function names
            if source.group(1).upper().startswith('CASE') or source.group(1).lstrip().upper().startswith('ROUND') \
                or source.group(1).upper().startswith("'") or source.group(1).upper().startswith('"'):
                table_name = ''
                column_name = source.group(1)
            else: 
                table_name, column_name = source.group(1).split('.')
            
            #alias  = re.search(' AS (.+?)', field.upper(), re.DOTALL).group(1)
            alias  = field[field.upper().find(' AS ') + 4:]
    #print("alias: ", alias)
    return (table_name, column_name, alias)


query1 = """
CREATE OR REPLACE VIEW rlsc_master.v_webmtbs_unified_view
AS
---without the DISTINCT, the time takes to generate the mv_v_ will be cut a lot, and the total number of records will not differ much
 SELECT protocol_instance.md_batch_name AS experiment_run_info_metid, protocol_instance.md_compound_name AS compound_name_metid, "substring"(protocol_instance.md_compound_name::text, 1, 11) AS parent_id_metid, protocol_instance.md_id AS protocol_instance_id_metid, clustered_compound.md_id AS clustered_compound_id_metid, compound_data.md_area_ms AS ms_signal_area_metid, compound_data.md_id AS compound_data_id_metid, compound_data.md_ion_formula AS metabolite_ion_formula_metid, compound_data.metabolite_mz_value_metid, compound_data.metabolite_mz_diff_ppm_metid, compound_data.metabolite_mz_diff_mda_metid, compound_data.md_name AS metabolite_name_metid, compound_data.metabolite_area_abs_metid, mim_calc.mim AS metabolite_mim_metid, mass_shift_calc.mass_shift AS mass_shift_metid, compound_data.retention_time_metid, compound_data.md_z AS metabolite_z_metid, 
        CASE WHEN compound_data.min_area_pct::text = compound_data.max_area_pct::text THEN compound_data.max_area_pct::text ELSE compound_data.metabolite_area_pct_metid END AS metabolite_area_pct_metid, 
		spectrum_value.md_id AS spectrum_id_metid, spectrum_value.md_name AS spectrum_name_metid, spectrum_value.md_mz AS spectrum_mz_value_metid, spectrum_value.md_signal AS spectrum_signal_value_metid, 
		round(spectrum_value.md_signal / CASE WHEN spectrum_value.max_md_signal = 0 THEN NULL ELSE spectrum_value.max_md_signal END * 100, 6) AS spectrum_signal_value_pct_metid, 
		molecule.md_cached_mol_file AS mol_string_metid, molecule.md_inchi AS inchi_string_metid, molecule.md_mim AS mim_value_metid, molecule.md_name AS molecule_name_metid, molecule.md_smiles AS molecule_smiles_metid, chroma_data.md_time AS chroma_time_value_metid, chroma_data.md_signal AS chroma_signal_value_metid, 
		round(chroma_data.md_signal / CASE WHEN chroma_data.max_md_signal = 0 THEN NULL ELSE chroma_data.max_md_signal END * 100, 6) AS chroma_signal_value_pct_metid, 
		unit.md_name AS unit_md_name_metid, pivot_table.time_metid, pivot_table.matrix_conc_metid, pivot_table.substrate_conc_metid, pivot_table.matrix_metid, pivot_table.requestor_metid, pivot_table.notebook_metid, pivot_table.req_rep_date_metid, pivot_table.program_metid, pivot_table.species_metid, pivot_table.met_id_analyst_metid,
		'http://webmetabase.merck.com:8080/WebMetabase/#open/'||protocol_instance.md_id as study_url_metid
   FROM rlsc_master.webmtbs_md_protocol_instance protocol_instance

   LEFT JOIN rlsc_master.mv_v_webmtbs_unified_view_compound_data compound_data ON protocol_instance.md_id = compound_data.md_protocol_instance_id
   LEFT JOIN rlsc_master.mv_v_webmtbs_unified_view_pivot_table pivot_table ON protocol_instance.md_id = pivot_table.md_protocol_instance_id
   LEFT JOIN rlsc_master.mv_v_webmtbs_unified_view_unit unit ON protocol_instance.md_id = unit.md_protocol_instance_id
   LEFT JOIN rlsc_master.webmtbs_md_compound_data compound_mol ON protocol_instance.md_substrate_id = compound_mol.md_id
   LEFT JOIN rlsc_master.webmtbs_md_molecule molecule ON compound_mol.md_molecule_id = molecule.md_id
   LEFT JOIN rlsc_master.webmtbs_md_experiment experiment ON compound_data.md_experiment_id = experiment.md_id
   LEFT JOIN rlsc_master.mv_v_webmtbs_unified_view_clustered_compound
 
   clustered_compound ON clustered_compound.compound_md_id = compound_data.md_id
   ---LEFT JOIN rlsc_master.webmtbs_md_compound_data compound_sub ON compound_sub.md_id = experiment.md_substrate_id
   LEFT JOIN rlsc_master.mv_v_webmtbs_unified_view_spectrum_value
 
	spectrum_value ON compound_data.md_id = spectrum_value.md_compound_id
   LEFT JOIN rlsc_master.mv_v_webmtbs_unified_view_chroma_data

	chroma_data ON compound_data.md_id = chroma_data.md_compound_id

     LEFT JOIN rlsc_master.mv_v_webmtbs_unified_view_calc_mim
 
   mim_calc ON experiment.md_protocol_instance_id = mim_calc.experiment_md_protocol_instance_id AND compound_data.md_id = mim_calc.compound_md_id AND compound_data.md_name = mim_calc.compound_md_name
     LEFT JOIN rlsc_master.mv_v_webmtbs_unified_view_calc_mass_shift

   mass_shift_calc ON experiment.md_protocol_instance_id = mass_shift_calc.experiment_md_protocol_instance_id AND compound_data.md_id = mass_shift_calc.compound_md_id AND compound_data.md_name = mass_shift_calc.compound_md_name
   WHERE protocol_instance.md_approved = 1
   with no schema binding;
"""

query2 = """
CREATE OR REPLACE VIEW rlsc_master.v_salar_aligned
AS
SELECT mv_v_aligned.*, mv_v_salar_toxicity_groups.label, mv_v_salar_toxicity_groups.preflabel, standardized_organism.standardized_value AS preferred_species
FROM
(
SELECT mv_v_salar_animal_txt_study.animal_txt, mv_v_salar_animal_txt_study.study,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.sex WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.sex WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.sex WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.sex WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.sex END as sex,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.species WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.species WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.species WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.species WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.species END as species,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.compound_prefname WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.compound_prefname WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.compound_prefname WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.compound_prefname WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.compound_prefname END as compound_prefname,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.startdate WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.startdate WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.startdate WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.startdate WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.startdate END as startdate,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.enddate WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.enddate WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.enddate WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.enddate WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.enddate END as enddate,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.title WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.title WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.title WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.title WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.title END as title,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.lab WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.lab WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.lab WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.lab WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.lab END as lab,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.experimenttype WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.experimenttype WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.experimenttype WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.experimenttype WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.experimenttype END as experimenttype,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.program WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.program WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.program WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.program WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.program END as program,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.lnumber WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.lnumber WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.lnumber WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.lnumber WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.lnumber END as lnumber,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.mknumber WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.mknumber WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.mknumber WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.mknumber WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.mknumber END as mknumber,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.commonname WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.commonname WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.commonname WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.commonname WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.commonname END as commonname,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.brandname WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.brandname WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.brandname WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.brandname WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.brandname END as brandname,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.sch WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.sch WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.sch WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.sch WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.sch END as sch,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.toxgroup WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.toxgroup WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.toxgroup WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.toxgroup WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.toxgroup END as toxgroup,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.studyDay WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.studyDay WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.studyDay END as studyDay,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.dose WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.dose WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.dose WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.dose WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.dose END as dose,
CASE WHEN mv_v_salar_clinical_signs.animal_txt IS NOT NULL THEN mv_v_salar_clinical_signs.doseUnit WHEN mv_v_salar_invivo_results.animal_txt IS NOT NULL THEN mv_v_salar_invivo_results.doseUnit WHEN mv_v_salar_food_consumption.animal_txt IS NOT NULL THEN mv_v_salar_food_consumption.doseUnit WHEN mv_v_salar_histopathology.animal_txt IS NOT NULL THEN mv_v_salar_histopathology.doseUnit WHEN mv_v_salar_necropsy.animal_txt IS NOT NULL THEN mv_v_salar_necropsy.doseUnit END as doseUnit,
---mv_v_salar_clinical_signs.dose as dose_clinical_signs,
---mv_v_salar_clinical_signs.doseUnit as doseUnit_clinical_signs,
mv_v_salar_clinical_signs.clinicalSignMeasurement,
---mv_v_salar_clinical_signs.studyDay as studyDay_clinical_signs,
mv_v_salar_clinical_signs.opthamicIndication,
mv_v_salar_clinical_signs.drugDay as drugDay_clinical_signs,
mv_v_salar_clinical_signs.drugWeek as drugWeek_clinical_signs,
mv_v_salar_clinical_signs.eyeCode,
mv_v_salar_clinical_signs.incidenceDate,
mv_v_salar_clinical_signs.normalIndication,
mv_v_salar_clinical_signs.modifier,
mv_v_salar_clinical_signs.category,
mv_v_salar_clinical_signs.subCategory,
mv_v_salar_clinical_signs.location,
mv_v_salar_clinical_signs.locationModifier,
mv_v_salar_clinical_signs.severity,
mv_v_salar_clinical_signs.sizeModifier,
mv_v_salar_clinical_signs.incidenceID,
mv_v_salar_clinical_signs.comment,
mv_v_salar_invivo_results.strain,
---mv_v_salar_invivo_results.dose as dose_invivo_results,
---mv_v_salar_invivo_results.doseUnit as doesUnit_invivo_results,
mv_v_salar_invivo_results.material_txt,
mv_v_salar_invivo_results.measurement as measurement_invivo_results,
mv_v_salar_invivo_results.valueQual as valueQual_invivo_results,
mv_v_salar_invivo_results.val as val_invivo_results,
mv_v_salar_invivo_results.unit as unit_invivo_results,
---mv_v_salar_invivo_results.studyDay as studyDay_invivo_results,
mv_v_salar_invivo_results.ExposureTime,
mv_v_salar_invivo_results.ExposureTimeUnits,
mv_v_salar_invivo_results.ExclusionFlag,
mv_v_salar_invivo_results.ReasonForExclusion,
---mv_v_salar_food_consumption.dose as dose_food_consumption,
---mv_v_salar_food_consumption.doseUnit as doseUnit_food_consumption,
mv_v_salar_food_consumption.AnimalAge,
mv_v_salar_food_consumption.FoodAmount,
mv_v_salar_food_consumption.FoodUnits,
mv_v_salar_food_consumption.FeedingDate,
---mv_v_salar_food_consumption.StudyDay as studyDay_food_consumption,
mv_v_salar_food_consumption.StudyWeek as studyWeek_food_consumption,
mv_v_salar_food_consumption.DrugDay as drugDay_food_consumption,
mv_v_salar_food_consumption.DrugWeek as drugWeek_food_consumption,
mv_v_salar_food_consumption.IntervalDay,
mv_v_salar_food_consumption.IntervalType,
---mv_v_salar_histopathology.material_txt as material_txt_histopathology,
---mv_v_salar_histopathology.dose as dose_histopathology,
---mv_v_salar_histopathology.doseUnit as doesUnit_histopatholoty,
mv_v_salar_histopathology.value as value_histopathology,
mv_v_salar_histopathology.observationType,
mv_v_salar_histopathology.adjective1,
mv_v_salar_histopathology.adjective2,
mv_v_salar_histopathology.Grade,
mv_v_salar_histopathology.GradeDescription,
mv_v_salar_histopathology.process,
mv_v_salar_necropsy.moribundAtNecropsy,
mv_v_salar_necropsy.necropsyDay,
mv_v_salar_necropsy.reasonDiscontinued,
mv_v_salar_necropsy.material_txt as material_txt_necropsy,
---mv_v_salar_necropsy.dose as dose_necropsy,
---mv_v_salar_necropsy.doseUnit as doesUnit_necropsy,
mv_v_salar_necropsy.measurement as measurement_necropsy,
mv_v_salar_necropsy.value as value_necropsy,
mv_v_salar_necropsy.unit as unit_necropsy,
mv_v_salar_necropsy.resultType as resulttype_necropsy,
mv_v_salar_invivo_results.resulttype as resulttype_invivo_results

FROM 
(
SELECT animal_txt, study, studyday
FROM rlsc_master.mv_v_salar_clinical_signs
UNION
SELECT animal_txt, study, studyday
FROM rlsc_master.mv_v_salar_invivo_results
UNION
SELECT animal_txt, study, studyday
FROM rlsc_master.mv_v_salar_food_consumption
UNION
SELECT animal_txt, study, NULL as studyday
FROM rlsc_master.mv_v_salar_necropsy
UNION
SELECT animal_txt, study, NULL as studyday
FROM rlsc_master.mv_v_salar_histopathology
) mv_v_salar_animal_txt_study
LEFT JOIN rlsc_master.mv_v_salar_clinical_signs
ON mv_v_salar_animal_txt_study.animal_txt = mv_v_salar_clinical_signs.animal_txt AND mv_v_salar_animal_txt_study.study = mv_v_salar_clinical_signs.study AND mv_v_salar_animal_txt_study.studyday = mv_v_salar_clinical_signs.studyday
LEFT JOIN rlsc_master.mv_v_salar_food_consumption
ON mv_v_salar_animal_txt_study.animal_txt = mv_v_salar_food_consumption.animal_txt AND mv_v_salar_animal_txt_study.study = mv_v_salar_food_consumption.study AND mv_v_salar_animal_txt_study.studyday = mv_v_salar_food_consumption.studyday
LEFT JOIN rlsc_master.mv_v_salar_histopathology
ON mv_v_salar_animal_txt_study.animal_txt = mv_v_salar_histopathology.animal_txt AND mv_v_salar_animal_txt_study.study = mv_v_salar_histopathology.study
LEFT JOIN rlsc_master.mv_v_salar_invivo_results
ON mv_v_salar_animal_txt_study.animal_txt = mv_v_salar_invivo_results.animal_txt AND mv_v_salar_animal_txt_study.study = mv_v_salar_invivo_results.study AND NVL(mv_v_salar_animal_txt_study.studyday, 1000) = NVL(mv_v_salar_invivo_results.studyday, 1000)
LEFT JOIN rlsc_master.mv_v_salar_necropsy
ON mv_v_salar_animal_txt_study.animal_txt = mv_v_salar_necropsy.animal_txt AND mv_v_salar_animal_txt_study.study = mv_v_salar_necropsy.study
) mv_v_aligned
LEFT JOIN rlsc_master.mv_v_salar_toxicity_groups
ON mv_v_aligned.study = mv_v_salar_toxicity_groups.study AND mv_v_aligned.lnumber = mv_v_salar_toxicity_groups.lnumber
LEFT JOIN rlsc_master.standardized_organism ON 
UPPER(mv_v_aligned.species)= standardized_organism.organism_name
with no schema binding;
"""


tokens = query2.split()
#print(tokens)

# get field names by taking portion of query between SELECT and FROM (exclusive)
m1 = re.search('SELECT (.+?) FROM', query1, re.DOTALL)
m1a = re.findall('SELECT (.+?) FROM', query2, re.MULTILINE)
print(type(m1a))

# get table names by taking portion of query between FROM and WHERE (exclusive)
m2 = re.search('FROM (.+?) WHERE', query2, re.DOTALL)

# split falls down on function calls (round, substring, etc) that take multiple arguments. would also fall down when string contains 
# comma.
# need a way to split on ',' when not within paranetheses and not within quotes. quotes is more complicated because opening and 
# closing quotes are the same character. need to track type of quote last encountered
#fields = m1.group(1).split(',')

fields2 = field_split(m1.group(1))
for field in fields2:
    field = field.lstrip()
    print(field)
    print(parse_field(field))

"""
fields = []
capture = False

for token in tokens:
    if capture:
        fields.append(token)
    if token == 'SELECT':
        print("SELECT found")
        capture = True
    if token == 'FROM':
        print("FROM encountered")
        capture = False

print(fields)
"""