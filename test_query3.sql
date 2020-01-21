SELECT mv_v_aligned.*, table2.label2, table2.label, table2.preflabel, standardized_organism.standardized_value AS preferred_species FROM
(
SELECT mv_v_salar_animal_txt_study.animal_txt, mv_v_salar_animal_txt_study.study,
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
FROM rlsc_master.mv_v_salar_food_consumption AS consumption
) mv_v_salar_animal_txt_study
LEFT JOIN rlsc_master.mv_v_salar_food_consumption
ON mv_v_salar_animal_txt_study.animal_txt = mv_v_salar_food_consumption.animal_txt
) mv_v_aligned
LEFT JOIN rlsc_master.mv_v_salar_toxicity_groups table2
ON mv_v_aligned.study = mv_v_salar_toxicity_groups.study
LEFT JOIN rlsc_master.standardized_organism ON 
UPPER(mv_v_aligned.species)= standardized_organism.organism_name                                
with no schema binding ; extra_token_to_prevent_index_error

