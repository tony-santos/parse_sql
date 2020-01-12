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

