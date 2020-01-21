CREATE OR REPLACE VIEW rlsc_master.v_webmtbs_unified_view
AS
---without the DISTINCT, the time takes to generate the mv_v_ will be cut a lot, and the total number of records will not differ much
 SELECT protocol_instance.md_batch_name AS experiment_run_info_metid, protocol_instance.md_compound_name AS compound_name_metid, "substring"(protocol_instance.md_compound_name::text, 1, 11) AS parent_id_metid, protocol_instance.md_id AS protocol_instance_id_metid, 'http://webmetabase.merck.com:8080/WebMetabase/#open/'||protocol_instance.md_id as study_url_metid
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

