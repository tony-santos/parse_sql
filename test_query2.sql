SELECT mv_v_aligned.*, mv_v_salar_toxicity_groups.label, mv_v_salar_toxicity_groups.preflabel, standardized_organism.standardized_value AS preferred_species FROM
(
SELECT mv_v_salar_animal_txt_study.animal_txt, mv_v_salar_animal_txt_study.study,
mv_v_salar_clinical_signs.clinicalSignMeasurement,
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
) mv_v_aligned
LEFT JOIN rlsc_master.mv_v_salar_toxicity_groups
ON mv_v_aligned.study = mv_v_salar_toxicity_groups.study
LEFT JOIN rlsc_master.standardized_organism ON 
UPPER(mv_v_aligned.species)= standardized_organism.organism_name                                
with no schema binding ; extra_toke_to_prevent_index_error

