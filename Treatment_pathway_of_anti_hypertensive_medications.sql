
--Step 0: create code table				
					/*
					CREATE TABLE public.htn_220119_code_table (
						domain varchar NULL,
						concept_id int4 NULL,
						concept_name varchar NULL,
						cond varchar NULL,
						drug_comb varchar null,
						cov_cond varchar,
						cov_drug varchar,
						exclude varchar,
						meas varchar,
						factor_con varchar,
						factor_drug varchar
					);
					*/

--Step 1: identify first hypertension(HTN) diagnosis and anti-HTN treatment
					/*
					create table htn_220119_step_01_htn_and_antihtn
							( 
							person_id bigserial,
							drug_concept_id int,
							drug_exposure_start_date date,
							drug_exposure_end_date date, 
							condition_start_date date,
							days_supply int
							);
					*/
					
 		      --insert into htn_220119_step_01_htn_and_antihtn
					select distinct de.person_id, 
								 de.drug_concept_id,
								 de.drug_exposure_start_date,
								 de.drug_exposure_end_date,
								 de.condition_start_date,
								 de.days_supply
					from (
										--identify patients with anti-HTN medications 
										select deic.person_id, 
													 deic.drug_concept_id,
													 hct2.concept_name,
													 deic.drug_exposure_start_date, 
													 drug_exposure_end_date, 
													 c.condition_start_date,
													 row_number () over (partition by c.person_id order by drug_exposure_start_date) as drug_rownum,
													 days_supply
									  from (
									  					--identify patients with HTN
									  					select 
														  			person_id, 
														  			condition_concept_id,
														  			condition_start_date, 
														  			row_number () over (partition by person_id order by condition_start_date) as cond_rownum
														  from public.condition_occurrence co
														  left join htn_220119_code_table hct on co.condition_concept_id = hct.concept_id 
											  			where hct.cond = 'htn'
											  	) c
									  left join public.drug_exposure_index_cohort_htn deic on c.person_id = deic.person_id
									  left join htn_220119_code_table hct2 on deic.drug_concept_id = hct2.concept_id
									  where c.cond_rownum = 1 and 
									  			hct2.drug_comb in ('acei', 'arb', 'arb_ccb', 'arb_ccb_du', 'arb_du', 'bb', 'ccb', 'du', 'others')
							  ) de
						order by person_id;
							  
				
					select count (distinct person_id) from htn_220119_step_01_htn_and_antihtn;
					select * from drug_exposure_index_cohort_htn;

					
--Step 2: add observation period and cohort end date
					/*
					create table htn_220119_step_02_obs_period_and_cohort_end_date
							( 
							person_id bigserial,
							index_date date,
							cohort_end_date date,
							observation_period_start_date date,
							observation_period_end_date date,
							condition_start_date date
							);
					*/
				
					--insert into htn_220119_step_02_obs_period_and_cohort_end_date 
					with add_observation_period as 
										(
										--identify patients with continuous visit records
										select dt.person_id, 
													 dt.drug_exposure_start_date as index_date, 
													 op.observation_period_start_date, 
													 op.observation_period_end_date, 
													 dt.condition_start_date
										from 	 (
														select person_id, min(drug_exposure_start_date) as drug_exposure_start_date, condition_start_date
										        from htn_220119_step_01_htn_and_antihtn dt
										        group by person_id, condition_start_date 
										        order by person_id
										        ) dt
										left join   observation_period_htn op on 
														op.person_id = dt.person_id and 
														(op.observation_period_start_date + interval '365 day') <= dt.drug_exposure_start_date and 
														(op.observation_period_end_date - interval '1095 day') >= dt.drug_exposure_start_date
										where drug_exposure_start_date >= condition_start_date and observation_period_start_date is not null and observation_period_end_date is not null
										order by person_id 
										)
					select person_id, 
								 index_date, 
								 cast (index_date + interval '1095 days' as date) as cohort_end_date, 
								 observation_period_start_date, 
								 observation_period_end_date,
								 condition_start_date
					from add_observation_period;	 								
							
				select count(distinct person_id) from htn_220119_step_02_obs_period_and_cohort_end_date;
		
				
				select distinct de.person_id, de.drug_concept_id, c.concept_name, drug_exposure_start_date,drug_exposure_end_date, days_supply
				from htn_220119_step_01_htn_and_antihtn de
				left join concept c on de.drug_concept_id = c.concept_id 
				left join htn_220119_code_table hct on de.drug_concept_id = hct.concept_id 
				where person_id in (select person_id from htn_220119_step_02_obs_period_and_cohort_end_date)
				order by de.person_id, drug_exposure_start_date
				
------R 로 전처리--------
				
--Step 3: preprocess step 2 results to identify combination therapy using R code
          -- after preprocessing, insert R results to the "htn_220119_step_03_patient_preprocessed"
				
					/*
							create table htn_220119_step_03_patient_preprocessed
							( 
							person_id bigserial,
							drug_concept_id varchar,
							concept_name varchar,
							drug_exposure_start_date date,
							drug_exposure_end_date date,
							days_supply int
							);
					*/
	
									
--step 4: select patients with continuous record				
					/*
				  create table htn_220119_step_04_continuous_record 
					(
					person_id bigserial,
					drug_concept_id varchar,
					concept_name varchar,
					drug_exposure_start_date date,
					drug_exposure_end_date date,
					days_supply int
					);
					*/
							
				 --insert into htn_220119_step_04_continuous_record
					with remove_pathway_after_cohort_end_date as 
								(
										select s3.*, s4.cohort_end_date 
										from public.htn_220119_step_03_patient_preprocessed s3
										left join public.htn_220119_step_02_obs_period_and_cohort_end_date s4 on s3.person_id = s4.person_id 
										where drug_exposure_start_date <= cohort_end_date 
								),
							 select_pathway_gt_3yr as 
								(
										select a.person_id, rpa.drug_concept_id, rpa.concept_name, rpa.drug_exposure_start_date, rpa.drug_exposure_end_date, rpa.days_supply
										from  (
													select person_id, min (drug_exposure_start_date) as minstart, max(drug_exposure_end_date) as minend
													from remove_pathway_after_cohort_end_date
													group by person_id
												  )a
										left join remove_pathway_after_cohort_end_date  rpa on a.person_id = rpa.person_id
										where minend >= minstart + interval '1095 days'
										order by person_id, drug_exposure_start_date
								),
								remove_date_lt870 as 
								(
											select *
											from select_pathway_gt_3yr
											where person_id not in (
																	select person_id
																	from (
																				select *, sum(days_supply) over (partition by person_id) as totday
																				from select_pathway_gt_3yr
																				)c
																	where totday < 875 )
								),
							remove_datediff_gt30 as 
								(
										select *
										from remove_date_lt870
										where person_id not in (
																select person_id
																from (
																				select *, lead(drug_exposure_start_date, 1) over (partition by person_id) - drug_exposure_end_date as datediff
																				from remove_date_lt870
																				order by person_id, drug_exposure_start_date
																			)b
																where datediff >= 30 )
								)
					select *
					from remove_datediff_gt30
					where days_supply >= 30;
				
		select count(distinct person_id)
		from htn_220119_step_04_continuous_record;
				
				
				
--Step 5: exclude patients who have major surgery or pregnant event
					/*
					create table htn_220119_step_05_exclude_patient
							( 
							person_id bigserial,
							drug_concept_id varchar,
							concept_name varchar,
							drug_exposure_start_date date,
							drug_exposure_end_date date,
							days_supply int
							);
					*/

		  --insert into htn_220119_step_05_exclude_patient
					with exclude_surgery as (
										select distinct s04.person_id 
										from public.htn_220119_step_04_continuous_record	s04
										left join htn_220119_step_02_obs_period_and_cohort_end_date s02 on s04.person_id = s02.person_id 
										left join procedure_occurrence po on s02.person_id = po.person_id and po.procedure_date <= s02.index_date 
										left join public.htn_220119_code_table ct on po.procedure_concept_id = ct.concept_id 
										where ct."exclude" = 'major_surgery'
										),
							 exclude_pregnancy_cond as (
										select distinct s04.person_id 
										from 	public.htn_220119_step_04_continuous_record s04
										left join htn_220119_step_02_obs_period_and_cohort_end_date s02 on s04.person_id = s02.person_id 
										left join condition_occurrence co on s02.person_id = co.person_id and co.condition_start_date <= s02.index_date 
										left join public.htn_220119_code_table ct on co.condition_concept_id = ct.concept_id 
										where ct."exclude" = 'pregnant'
										),
							 exclude_pregnancy_meas as (
										select distinct s04.person_id 
										from public.htn_220119_step_04_continuous_record	s04
										left join htn_220119_step_02_obs_period_and_cohort_end_date s02 on s04.person_id = s02.person_id 
										left join measurement m on s02.person_id = m.person_id and m.measurement_date <= s02.index_date  
										left join public.htn_220119_code_table ct on m.measurement_concept_id = ct.concept_id 
										where ct."exclude" = 'hcg' and m.value_as_number >= 5
										),
							exclude_young as (
										select person_id
										from (
													select distinct s04.person_id, extract( year from s02.index_date) - p.year_of_birth as age_at_index
													from public.htn_220119_step_04_continuous_record	s04
													left join htn_220119_step_02_obs_period_and_cohort_end_date s02 on s04.person_id = s02.person_id 
													left join person p on s04.person_id = p.person_id 
													)a
										where age_at_index < 18		
										)
					select *
					from htn_220119_step_04_continuous_record s04
					where s04.person_id not in (select person_id from exclude_surgery union all
																				select person_id from exclude_pregnancy_cond union all
																				select person_id from exclude_pregnancy_meas union all
																			  select person_id from exclude_young);


				select count(distinct person_id) from htn_220119_step_05_exclude_patient;

--Step 6: identify pateints with predose and postdose blood pressure records
					/*
					create table htn_220119_step_06_pre_post_bp_record
							( 
							person_id bigserial,
							index_date date,
							cohort_end_date date,
							drug_concept_id varchar,
							concept_name varchar,
							drug_exposure_start_date date,
							drug_exposure_end_date date,
							days_supply int,
							sbp_pre_date date,
							dbp_pre_date date,
							sbp_pre_value int,
							dbp_pre_value int
							);
					 */
				
																			
					--insert into htn_220119_step_06_pre_post_bp_record																
			
				with sbp_pre_dose as 
							(
										select *
										from 	(
													-- identify patients with predose sbp
													select s3.person_id, 
																 s02.index_date, 
																 s02.cohort_end_date, 
																 mbp.measurement_concept_id, 
																 mbp.measurement_date, 
																 avg(mbp.value_as_number) as value_as_number, 
																 row_number () over (partition by s3.person_id order by measurement_date desc) as rownum
													from htn_220119_step_05_exclude_patient s3
													left join htn_220119_step_02_obs_period_and_cohort_end_date s02 on s3.person_id = s02.person_id 
													left join htn_210809_measurement_blood_pressure mbp on s3.person_id = mbp.person_id and mbp.measurement_date <= s02.index_date
													left join htn_220119_code_table hct on mbp.measurement_concept_id = hct.concept_id 
													where value_as_number is not null and hct.meas = 'sbp'
													group by s3.person_id, index_date, cohort_end_date, measurement_concept_id, measurement_date
													order by person_id 
												)a
										where rownum = 1
							),
							dbp_pre_dose as 
								(
										select *
										from (
													--identify patients with predose dbp
													select s3.person_id, 
																 s02.index_date, 
																 s02.cohort_end_date, 
																 mbp.measurement_concept_id, 
																 mbp.measurement_date, 
																 avg(mbp.value_as_number) as value_as_number, 
																 row_number () over (partition by s3.person_id order by measurement_date desc) as rownum
													from htn_220119_step_05_exclude_patient s3
													left join htn_220119_step_02_obs_period_and_cohort_end_date s02 on s3.person_id = s02.person_id
													left join htn_210809_measurement_blood_pressure mbp on s3.person_id = mbp.person_id and mbp.measurement_date <= s02.index_date
													left join htn_220119_code_table hct on mbp.measurement_concept_id = hct.concept_id 
													where value_as_number is not null and hct.meas = 'dbp'
													group by s3.person_id, index_date, cohort_end_date, measurement_concept_id, measurement_date
													order by person_id 
												)a
										where rownum = 1
								)
					select distinct s5ep.person_id,
								 s02.index_date,
								 s02.cohort_end_date,
								 s5ep.drug_concept_id,
								 s5ep.concept_name,
								 s5ep.drug_exposure_start_date,
								 s5ep.drug_exposure_end_date,
								 s5ep.days_supply,
								 sbppre.measurement_date as sbp_pre_date,
								 dbppre.measurement_date as dbp_pre_date,
								 sbppre.value_as_number as sbp_pre_value,
								 dbppre.value_as_number as dbp_pre_value
					from htn_211201_step_05_exclude_patient s5ep
					left join htn_220119_step_02_obs_period_and_cohort_end_date s02 on s5ep.person_id = s02.person_id
					left join sbp_pre_dose sbppre on s5ep.person_id = sbppre.person_id
					left join dbp_pre_dose dbppre on s5ep.person_id = dbppre.person_id
					where  sbppre.value_as_number is not null and 
								 dbppre.value_as_number is not null
					order by person_id ;
														
					select * from htn_220119_step_06_pre_post_bp_record;
					select count(distinct person_id) from htn_220119_step_06_pre_post_bp_record;
				

--Step 7: use Step 6 results to identify sequence of a drug per person using R code.
         --after identifying a sequence for drugs, import results into "htn_220119_step_07_treatment_pathway_seq_for_drug"
					/*
					create table htn_220119_step_07_treatment_pathway_seq_for_drug
							( 
							person_id bigserial,
							index_date date,
							cohort_end_date date,
							drug_concept_id varchar,
							concept_name varchar,
							drug_exposure_start_date date,
							drug_exposure_end_date date,
							days_supply int,
							sbp_pre_date date,
							dbp_pre_date date,
							sbp_pre_value int,
							dbp_pre_value int,
							seq_for_drug int
							);
					 */				
				
--Step 8: step 8_identify changed and unchanged group
					/*		
					create table htn_220119_step_08_drug_sequence_changed_unchanged
							(
							person_id bigserial,
							t_c varchar,
							index_date date,
							cohort_end_date date,
							drug_concept_id varchar,
							concept_name varchar,
							drug_exposure_start_date date,
							drug_exposure_end_date date,
							days_supply int,
							sbp_pre_date date,
							dbp_pre_date date,
							sbp_pre_value int,
							dbp_pre_value int,
							seq_for_drug int
							);			
					*/				
				
					--insert into htn_220119_step_08_drug_sequence_changed_unchanged
					with unchanged as 
							(
										select 
													distinct person_id, 
													'unchanged' as t_c,
													index_date,
													cohort_end_date,
													drug_concept_id, 
													concept_name, 
													min(drug_exposure_start_date) over (partition by person_id, seq_for_drug) as drug_exposure_start_date, 
													max(drug_exposure_end_date) over (partition by person_id, seq_for_drug) as drug_exposure_end_date,
													sum (days_supply) over (partition by person_id, seq_for_drug) as days_supply, 
													sbp_pre_date,
													dbp_pre_date,
													sbp_pre_value,
													dbp_pre_value,
													seq_for_drug 
										from public.htn_220119_step_07_treatment_pathway_seq_for_drug
										where person_id not in (select person_id from htn_220119_step_07_treatment_pathway_seq_for_drug where seq_for_drug = 2)
							 			group by person_id, index_date, cohort_end_date, drug_exposure_start_date, drug_exposure_end_date, drug_concept_id, concept_name, days_supply, 
							 			sbp_pre_date,	dbp_pre_date,	sbp_pre_value, dbp_pre_value, seq_for_drug 
										order by person_id 
							),
							changed as 
							(
										select 
													distinct person_id, 
													'changed' as t_c,
													index_date,
													cohort_end_date,
													drug_concept_id, 
													concept_name, 
													min(drug_exposure_start_date) over (partition by person_id, seq_for_drug) as drug_exposure_start_date, 
													max(drug_exposure_end_date) over (partition by person_id, seq_for_drug) as drug_exposure_end_date,
													sum (days_supply) over (partition by person_id, seq_for_drug) as days_supply, 
													sbp_pre_date,
													dbp_pre_date,
													sbp_pre_value,
													dbp_pre_value,
													seq_for_drug 
										from public.htn_220119_step_07_treatment_pathway_seq_for_drug
										where person_id not in (select person_id from unchanged)
										group by person_id, index_date, cohort_end_date, drug_exposure_start_date, drug_exposure_end_date, drug_concept_id, concept_name, days_supply, 
							 			sbp_pre_date,	dbp_pre_date,	sbp_pre_value, dbp_pre_value, seq_for_drug 
										order by person_id 
							)
							select *
							from unchanged
										union all
							select *
							from changed
		

					select * from htn_220119_step_08_drug_sequence_changed_unchanged;
					select count(distinct person_id) from htn_220119_step_08_drug_sequence_changed_unchanged;
							
--Step 9: identify conditions for covariate in target group
					/*		
					create table htn_220119_step_09_covariate_conditions_for_target
							(
							person_id bigserial,
							t_c varchar,
							condition_concept_id int,
							concept_name varchar
							);			
					*/		

					--insert into htn_220119_step_09_covariate_conditions_for_target
					select distinct s08.person_id, s08.t_c, co.condition_concept_id, c2.concept_name 
					from htn_220119_step_08_drug_sequence_changed_unchanged s08
					left join condition_occurrence co on s08.person_id = co.person_id and condition_start_date between index_date - interval '365 days' and index_date
					left join concept c2 on co.condition_concept_id = c2.concept_id 
					where t_c = 'changed'
					order by person_id
				
					select concept_name, count(concept_name)as cnt
					from htn_220119_step_09_covariate_conditions_for_target
					group by concept_name
					order by cnt desc
					


--Step 10: identify drug for covariate in target group
					/*
					create table htn_220119_step_10_covariate_drug_for_target
							(
							person_id bigserial,
							t_c varchar,
							index_date date,
							drug_concept_id int,
							concept_name varchar
							);			
					*/		
					
					--insert into htn_220119_step_10_covariate_drug_for_target
					select distinct s09.person_id, s09.t_c, s09.index_date, de.drug_concept_id, c2.concept_name 
					from htn_220119_step_08_drug_sequence_changed_unchanged s09
					left join drug_exposure de on s09.person_id = de.person_id and de.drug_exposure_start_date between s09.index_date - interval '365 days' and s09.index_date
					left join concept c2 on de.drug_concept_id = c2.concept_id 
					where t_c = 'changed'


					select concept_name, count(concept_name) as cnt
					from htn_220119_step_10_covariate_drug_for_target
					group by concept_name
					order by cnt desc
					
					
--Step 11: identify measurement for covariate in target group
					
					create table htn_220119_step_11_covariate_measurement_for_target
							(
							person_id bigserial,
							t_c varchar,
							index_date date,
							measurement_concept_id int,
							concept_name varchar,
							value_as_number numeric
							);			
					*/		

					--insert into htn_220119_step_11_covariate_measurement_for_target
					select distinct s09.person_id, s09.t_c, s09.index_date, m.measurement_concept_id, c2.concept_name, m.value_as_number 
					from htn_220119_step_08_drug_sequence_changed_unchanged s09
					left join measurement m on s09.person_id = m.person_id and m.measurement_date between s09.index_date - interval '365 days' and s09.index_date
					left join concept c2 on m.measurement_concept_id = c2.concept_id 
					where t_c = 'changed'


--Step 12: identify conditions for covariate in comparator group
					/*
					create table htn_220119_step_12_covariate_conditions_for_comparator
							(
							person_id bigserial,
							t_c varchar,
							index_date date,
							condition_concept_id int,
							concept_name varchar
							);			
					*/		

					--insert into htn_220119_step_12_covariate_conditions_for_comparator
					select distinct s09.person_id, s09.t_c, s09.index_date, co.condition_concept_id, c2.concept_name 
					from htn_220119_step_08_drug_sequence_changed_unchanged s09
					left join condition_occurrence co on s09.person_id = co.person_id and condition_start_date between s09.index_date - interval '365 days' and s09.index_date
					left join concept c2 on co.condition_concept_id = c2.concept_id 
					where t_c = 'unchanged'



--Step 13: identify drug for covariate in comparator group
					/*
					create table htn_220119_step_13_covariate_drug_for_comparator
							(
							person_id bigserial,
							t_c varchar,
							index_date date,
							drug_concept_id int,
							concept_name varchar
							);			
					*/		

					--insert into htn_220119_step_13_covariate_drug_for_comparator
					select distinct s09.person_id, s09.t_c, s09.index_date, de.drug_concept_id, c2.concept_name 
					from htn_220119_step_08_drug_sequence_changed_unchanged s09
					left join drug_exposure de on s09.person_id = de.person_id and de.drug_exposure_start_date between s09.index_date - interval '365 days' and s09.index_date
					left join concept c2 on de.drug_concept_id = c2.concept_id 
					where t_c = 'unchanged'



--Step 14: identify measurement for covariate in comparator group
					/*
					create table htn_220119_step_14_covariate_measurement_for_comparator
							(
							person_id bigserial,
							t_c varchar,
							index_date date,
							measurement_concept_id int,
							concept_name varchar,
							value_as_number numeric
							);			
					*/		
					
					--insert into htn_220119_step_14_covariate_measurement_for_comparator
					select distinct s09.person_id, s09.t_c, s09.index_date, m.measurement_concept_id, c2.concept_name, m.value_as_number 
					from htn_220119_step_08_drug_sequence_changed_unchanged s09
					left join measurement m on s09.person_id = m.person_id and m.measurement_date between s09.index_date - interval '365 days' and s09.index_date
					left join concept c2 on m.measurement_concept_id = c2.concept_id 
					where t_c = 'unchanged'


					
					
--Step 15: tibble table for ps matching
					/*
					create table htn_220119_step_15_covariate_tibble
								(
								person_id bigserial,
								t_c varchar,
								index_year int, 
								age int,
								gender varchar,
								abnormal_ecg varchar,
								af varchar,
								amyloidosis varchar,
								angina_pectoris varchar,
								arrhythmia varchar,
								arteriosclerosis_obliterans varchar,
								bladder_disease varchar,
								cardiomyopathy varchar,
								cardiovascular_disease varchar,
								cerebrovascular_disease varchar,
								coronary_arteriosclerosis varchar,
								diabetes_mellitus varchar,
								diabetic_neuropathy varchar,
								dyslipidemia varchar,
								dyspnea varchar,
								eye_disease varchar,
								gi_disease varchar,
								heart_failure varchar,
								hyperuricemia varchar,
								hypotension varchar,
								hypothyroidism varchar,
								insomnia varchar,
								liver_disease varchar,
								mental_and_behavioural_disorder varchar,
								musculoskeletal_disease varchar,
								neoplasm varchar,
								nervous_system_disorder varchar,
								nstemi varchar,
								obesity varchar,
								prediabetes varchar,
								prostatic_hyperplasia varchar,
								pulmonary_disease varchar,
								renal_disease varchar,
								symptom varchar,
								thyroid_disease varchar,
								analgesic_drug varchar,
								antianginal_agent varchar,
								antianxiety_agent varchar,
								antiarrhythmic_agent varchar,
								antibacterials varchar,
								anticoagulant_drug varchar,
								anticonvulsant varchar,
								antidepressant varchar,
								antidiabetic_agent varchar,
								antigout_agent varchar,
								antihyperlipidemic_agent varchar,
								antithyroid_agent varchar,
								antiviral_agent varchar,
								gastrointestinal_agent varchar,
								hmg varchar,
								immunosuppressive_agent varchar,
								micturition_disorder_drug varchar,
								nsaids varchar,
								respiratory_system_drug varchar,
								sedatives varchar,
								sbp_pre_value int,
								dbp_pre_value int
								);	
								*/

					--insert into htn_220119_step_15_covariate_tibble
					with factor_con as 
							(
							select 
										distinct person_id, 
										t_c, 
										case when (cov_cond = 'abnormal ecg') then 1 else 0 end as abnormal_ecg,
										case when (cov_cond = 'af') then 1 else 0 end as af,
										case when (cov_cond = 'amyloidosis') then 1 else 0 end as amyloidosis,
										case when (cov_cond = 'angina pectoris') then 1 else 0 end as angina_pectoris,
										case when (cov_cond = 'arrhythmia') then 1 else 0 end as arrhythmia,
										case when (cov_cond = 'arteriosclerosis obliterans') then 1 else 0 end as arteriosclerosis_obliterans,
										case when (cov_cond = 'bladder disease') then 1 else 0 end as bladder_disease,
										case when (cov_cond = 'cardiomyopathy') then 1 else 0 end as cardiomyopathy,
										case when (cov_cond = 'cardiovascular disease') then 1 else 0 end as cardiovascular_disease,
										case when (cov_cond = 'cerebrovascular disease') then 1 else 0 end as cerebrovascular_disease,
										case when (cov_cond = 'coronary arteriosclerosis') then 1 else 0 end as coronary_arteriosclerosis,
										case when (cov_cond = 'diabetes mellitus') then 1 else 0 end as diabetes_mellitus,
										case when (cov_cond = 'diabetic neuropathy') then 1 else 0 end as diabetic_neuropathy,
										case when (cov_cond = 'dyslipidemia') then 1 
												 when (meas = 'tot_chol' and value_as_number >=240) then 1
												 when (meas = 'hdl' and value_as_number <40) then 1
												 when (meas = 'ldl' and value_as_number >=160) then 1
												 when (meas = 'tg' and value_as_number >200) then 1 else 0 end as dyslipidemia,
										case when (cov_cond = 'dyspnea') then 1 else 0 end as dyspnea,
										case when (cov_cond = 'eye disease') then 1 else 0 end as eye_disease,
										case when (cov_cond = 'gi disease') then 1 else 0 end as gi_disease,
										case when (cov_cond = 'heart failure') then 1 else 0 end as heart_failure,
										case when (cov_cond = 'hyperuricemia') then 1 else 0 end as hyperuricemia,
										case when (cov_cond = 'hypotension') then 1 else 0 end as hypotension,
										case when (cov_cond = 'hypothyroidism') then 1 else 0 end as hypothyroidism,
										case when (cov_cond = 'insomnia') then 1 else 0 end as insomnia,
										case when (cov_cond = 'liver disease') then 1
												 when (meas = 'ast' and value_as_number >=120) then 1
												 when (meas = 'alt' and value_as_number >=120) then 1 else 0 end as liver_disease,
										case when (cov_cond = 'mental and behavioural disorder') then 1 else 0 end as mental_and_behavioural_disorder,
										case when (cov_cond = 'musculoskeletal disease') then 1 else 0 end as musculoskeletal_disease,
										case when (cov_cond = 'neoplasm') then 1 else 0 end as neoplasm,
										case when (cov_cond = 'nervous system disorder') then 1 else 0 end as nervous_system_disorder,
										case when (cov_cond = 'nstemi') then 1 else 0 end as nstemi,
										case when (cov_cond = 'obesity') then 1 else 0 end as obesity,
										case when (cov_cond = 'prediabetes') then 1 else 0 end as prediabetes,
										case when (cov_cond = 'prostatic hyperplasia') then 1 else 0 end as prostatic_hyperplasia,
										case when (cov_cond = 'pulmonary disease') then 1 else 0 end as pulmonary_disease,
										case when (cov_cond = 'renal disease') then 1
												 when (meas = 'creatinine' and value_as_number > 1.4) then 1
												 when (meas = 'gfr' and value_as_number < 60) then 1 else 0 end as renal_disease,
										case when (cov_cond = 'symptom') then 1 else 0 end as symptom,
										case when (cov_cond = 'thyroid_disease') then 1 else 0 end as thyroid_disease
							from (
										select distinct s09.person_id, s09.t_c, ct.cov_cond, s12.value_as_number, ct2.meas, p.gender_source_value 
										from htn_220119_step_08_drug_sequence_changed_unchanged s09
										left join public.htn_220119_step_09_covariate_conditions_for_target s10 on s09.person_id = s10.person_id 
										left join htn_220119_code_table ct on s10.condition_concept_id = ct.concept_id 
										left join htn_220119_step_11_covariate_measurement_for_target s12 on s09.person_id = s12.person_id 
										left join htn_220119_code_table ct2 on s12.measurement_concept_id = ct2.concept_id
										left join person p on s09.person_id = p.person_id 
										where ct.cov_cond != 'NULL'
													union all 
										select distinct s09.person_id, s09.t_c, ct.cov_cond, s15.value_as_number, ct2.meas, p.gender_source_value 
										from htn_220119_step_08_drug_sequence_changed_unchanged s09
										left join public.htn_220119_step_12_covariate_conditions_for_comparator s13 on s09.person_id = s13.person_id 
										left join htn_220119_code_table ct on s13.condition_concept_id = ct.concept_id 
										left join htn_220119_step_14_covariate_measurement_for_comparator s15 on s09.person_id = s15.person_id 
										left join htn_220119_code_table ct2 on s15.measurement_concept_id = ct2.concept_id
										left join person p on s09.person_id = p.person_id 
										where ct.cov_cond != 'NULL'													
										)a
							order by person_id
							),
							cov_drug as
							(
							select 
										distinct person_id, 
										t_c, 
										case when (cov_drug = 'analgesic drug') then 1 else 0 end as analgesic_drug,
										case when (cov_drug = 'antianginal agent') then 1 else 0 end as antianginal_agent,
										case when (cov_drug = 'antianxiety agent') then 1 else 0 end as antianxiety_agent,
										case when (cov_drug = 'antiarrhythmic agent') then 1 else 0 end as antiarrhythmic_agent,
										case when (cov_drug = 'antibacterial') then 1 else 0 end as antibacterial,
										case when (cov_drug = 'anticoagulant drug') then 1 else 0 end as anticoagulant_drug,
										case when (cov_drug = 'anticonvulsant') then 1 else 0 end as anticonvulsant,
										case when (cov_drug = 'antidepressant') then 1 else 0 end as antidepressant,
										case when (cov_drug = 'antidiabetic agent') then 1 else 0 end as antidiabetic_agent,
										case when (cov_drug = 'antigout agent') then 1 else 0 end as antigout_agent,
										case when (cov_drug = 'antihyperlipidemic agent') then 1 else 0 end as antihyperlipidemic_agent,
										case when (cov_drug = 'antithyroid agent') then 1 else 0 end as antithyroid_agent,
										case when (cov_drug = 'antiviral_agent') then 1 else 0 end as antiviral_agent,
										case when (cov_drug = 'gastrointestinal agent') then 1 else 0 end as gastrointestinal_agent,
										case when (cov_drug = 'hmg') then 1 else 0 end as hmg,
										case when (cov_drug = 'immunosuppressive agent') then 1 else 0 end as immunosuppressive_agent,
										case when (cov_drug = 'micturition disorder drug') then 1 else 0 end as micturition_disorder_drug,
										case when (cov_drug = 'nsaids') then 1 else 0 end as nsaids,
										case when (cov_drug = 'respiratory system drug') then 1 else 0 end as respiratory_system_drug,
										case when (cov_drug = 'sedatives') then 1 else 0 end as sedatives
							from					
										(
										select distinct s09.person_id, s09.t_c, ct.cov_drug
										from htn_220119_step_08_drug_sequence_changed_unchanged s09
										left join public.htn_220119_step_10_covariate_drug_for_target s11 on s09.person_id = s11.person_id 
										left join htn_220119_code_table ct on s11.drug_concept_id = ct.concept_id 
										where ct.cov_drug != 'NULL'		
													union all 
										select distinct s09.person_id, s09.t_c, ct.cov_drug
										from htn_220119_step_08_drug_sequence_changed_unchanged s09
										left join public.htn_220119_step_13_covariate_drug_for_comparator s14 on s09.person_id = s14.person_id 
										left join htn_220119_code_table ct on s14.drug_concept_id = ct.concept_id 
										where ct.cov_drug != 'NULL'															
										)b
							order by person_id
							)
					select 
								distinct s09.person_id, 
								s09.t_c,
								extract (year from s09.index_date) as index_year,  
								max (extract (year from s09.drug_exposure_start_date) - p.year_of_birth) over (partition by s09.person_id) as age, 
								p.gender_source_value as gender,
								max(abnormal_ecg) over (partition by s09.person_id) as abnormal_ecg,
								max(af) over (partition by s09.person_id) as af,
								max(amyloidosis) over (partition by s09.person_id) as amyloidosis,
								max(angina_pectoris) over (partition by s09.person_id) as angina_pectoris,
								max(arrhythmia) over (partition by s09.person_id) as arrhythmia,
								max(arteriosclerosis_obliterans) over (partition by s09.person_id) as arteriosclerosis_obliterans,
								max(bladder_disease) over (partition by s09.person_id) as bladder_disease,
								max(cardiomyopathy) over (partition by s09.person_id) as cardiomyopathy,
								max(cardiovascular_disease) over (partition by s09.person_id) as cardiovascular_disease,
								max(cerebrovascular_disease) over (partition by s09.person_id) as cerebrovascular_disease,
								max(coronary_arteriosclerosis) over (partition by s09.person_id) as coronary_arteriosclerosis,
								max(diabetes_mellitus) over (partition by s09.person_id) as diabetes_mellitus,
								max(diabetic_neuropathy) over (partition by s09.person_id) as diabetic_neuropathy,
								max(dyslipidemia) over (partition by s09.person_id) as dyslipidemia,
								max(dyspnea) over (partition by s09.person_id) as dyspnea,
								max(eye_disease) over (partition by s09.person_id) as eye_disease,
								max(gi_disease) over (partition by s09.person_id) as gi_disease,
								max(heart_failure) over (partition by s09.person_id) as heart_failure,
								max(hyperuricemia) over (partition by s09.person_id) as hyperuricemia,
								max(hypotension) over (partition by s09.person_id) as hypotension,
								max(hypothyroidism) over (partition by s09.person_id) as hypothyroidism,
								max(insomnia) over (partition by s09.person_id) as insomnia,
								max(liver_disease) over (partition by s09.person_id) as liver_disease,
								max(mental_and_behavioural_disorder) over (partition by s09.person_id) as mental_and_behavioural_disorder,
								max(musculoskeletal_disease) over (partition by s09.person_id) as musculoskeletal_disease,
								max(neoplasm) over (partition by s09.person_id) as neoplasm,
								max(nervous_system_disorder) over (partition by s09.person_id) as nervous_system_disorder,
								max(nstemi) over (partition by s09.person_id) as nstemi,
								max(obesity) over (partition by s09.person_id) as obesity,
								max(prediabetes) over (partition by s09.person_id) as prediabetes,
								max(prostatic_hyperplasia) over (partition by s09.person_id) as prostatic_hyperplasia,
								max(pulmonary_disease) over (partition by s09.person_id) as pulmonary_disease,
								max(renal_disease) over (partition by s09.person_id) as renal_disase,
								max(symptom) over (partition by s09.person_id) as symptom,
								max(thyroid_disease) over (partition by s09.person_id) as thyroid_disease,
								max(analgesic_drug) over (partition by s09.person_id) as analgesic_drug,
								max(antianginal_agent) over (partition by s09.person_id) as antianginal_agent,
								max(antianxiety_agent) over (partition by s09.person_id) as antianxiety_agent,
								max(antiarrhythmic_agent) over (partition by s09.person_id) as antiarrhythmic_agent,
								max(antibacterial) over (partition by s09.person_id) as antibacterial,								
								max(anticoagulant_drug) over (partition by s09.person_id) as anticoagulant_drug,
								max(anticonvulsant) over (partition by s09.person_id) as anticonvulsant,
								max(antidepressant) over (partition by s09.person_id) as antidepressant,
								max(antidiabetic_agent) over (partition by s09.person_id) as antidiabetic_agent,
								max(antigout_agent) over (partition by s09.person_id) as antigout_agent,
								max(antihyperlipidemic_agent) over (partition by s09.person_id) as antihyperlipidemic_agent,
								max(antithyroid_agent) over (partition by s09.person_id) as antithyroid_agent,
								max(antiviral_agent) over (partition by s09.person_id) as antiviral_agent,
								max(gastrointestinal_agent) over (partition by s09.person_id) as gastrointestinal_agent,
								max(hmg) over (partition by s09.person_id) as hmg,
								max(immunosuppressive_agent) over (partition by s09.person_id) as immunosuppressive_agent,
								max(micturition_disorder_drug) over (partition by s09.person_id) as micturition_disorder_drug,
								max(nsaids) over (partition by s09.person_id) as nsaids,
								max(respiratory_system_drug) over (partition by s09.person_id) as respiratory_system_drug,
								max(sedatives) over (partition by s09.person_id) as sedatives,
								s04.sbp_pre_value,
								s04.dbp_pre_value 
					from htn_220119_step_08_drug_sequence_changed_unchanged s09
					left join person p on s09.person_id = p.person_id 
					left join factor_con cc on s09.person_id = cc.person_id
					left join cov_drug cd on s09.person_id = cd.person_id
					left join public.htn_220119_step_06_pre_post_bp_record s04 on s09.person_id = s04.person_id 
					order by person_id;
							
				
				
--Step 16: sensitivity_analysis_identify conditions for covariate in target group
					/*
					create table htn_220119_step_16_covariate_conditions_for_target_sensitivity
							(
							person_id bigserial,
							t_c varchar,
							condition_concept_id int,
							concept_name varchar
							);			
					*/		

					--insert into htn_220119_step_16_covariate_conditions_for_target_sensitivity
					select distinct s08.person_id, s08.t_c, co.condition_concept_id, c2.concept_name 
					from htn_220119_step_08_drug_sequence_changed_unchanged s08
					left join condition_occurrence co on s08.person_id = co.person_id and condition_start_date between index_date - interval '180 days' and index_date
					left join concept c2 on co.condition_concept_id = c2.concept_id 
					where t_c = 'changed'
					order by person_id
				
					select concept_name, count(concept_name)as cnt
					from htn_220119_step_09_covariate_conditions_for_target
					group by concept_name
					order by cnt desc
					


--Step 17: sensitivity_analysis_identify drug for covariate in target group
					/*
					create table htn_220119_step_17_covariate_drug_for_target_sensitivity
							(
							person_id bigserial,
							t_c varchar,
							index_date date,
							drug_concept_id int,
							concept_name varchar
							);			
					*/		
					
					--insert into htn_220119_step_17_covariate_drug_for_target_sensitivity
					select distinct s09.person_id, s09.t_c, s09.index_date, de.drug_concept_id, c2.concept_name 
					from htn_220119_step_08_drug_sequence_changed_unchanged s09
					left join drug_exposure de on s09.person_id = de.person_id and de.drug_exposure_start_date between s09.index_date - interval '180 days' and s09.index_date
					left join concept c2 on de.drug_concept_id = c2.concept_id 
					where t_c = 'changed'
					
--Step 18: sensitivity_analysis_identify measurement for covariate in target group
					/*
					create table htn_220119_step_18_covariate_measurement_for_target_sensitivity
							(
							person_id bigserial,
							t_c varchar,
							index_date date,
							measurement_concept_id int,
							concept_name varchar,
							value_as_number numeric
							);			
					*/		

					--insert into htn_220119_step_18_covariate_measurement_for_target_sensitivity
					select distinct s09.person_id, s09.t_c, s09.index_date, m.measurement_concept_id, c2.concept_name, m.value_as_number 
					from htn_220119_step_08_drug_sequence_changed_unchanged s09
					left join measurement m on s09.person_id = m.person_id and m.measurement_date between s09.index_date - interval '180 days' and s09.index_date
					left join concept c2 on m.measurement_concept_id = c2.concept_id 
					where t_c = 'changed'


--Step 19: identify conditions for covariate in comparator group
					/*
					create table htn_220119_step_19_covariate_conditions_for_comparator_sensitivity
							(
							person_id bigserial,
							t_c varchar,
							index_date date,
							condition_concept_id int,
							concept_name varchar
							);			
					*/		

					--insert into htn_220119_step_19_covariate_conditions_for_comparator_sensitivity
					select distinct s09.person_id, s09.t_c, s09.index_date, co.condition_concept_id, c2.concept_name 
					from htn_220119_step_08_drug_sequence_changed_unchanged s09
					left join condition_occurrence co on s09.person_id = co.person_id and condition_start_date between s09.index_date - interval '180 days' and s09.index_date
					left join concept c2 on co.condition_concept_id = c2.concept_id 
					where t_c = 'unchanged'



--Step 20: sensitivity_analysis_identify drug for covariate in comparator group
					/*
					create table htn_220119_step_20_covariate_drug_for_comparator_sensitivity
							(
							person_id bigserial,
							t_c varchar,
							index_date date,
							drug_concept_id int,
							concept_name varchar
							);			
					*/		

					--insert into htn_220119_step_20_covariate_drug_for_comparator_sensitivity
					select distinct s09.person_id, s09.t_c, s09.index_date, de.drug_concept_id, c2.concept_name 
					from htn_220119_step_08_drug_sequence_changed_unchanged s09
					left join drug_exposure de on s09.person_id = de.person_id and de.drug_exposure_start_date between s09.index_date - interval '180 days' and s09.index_date
					left join concept c2 on de.drug_concept_id = c2.concept_id 
					where t_c = 'unchanged'



--Step 21: sensitivity_analysis_identify measurement for covariate in comparator group
					/*
					create table htn_220119_step_21_covariate_measurement_for_comparator_sensitivity
							(
							person_id bigserial,
							t_c varchar,
							index_date date,
							measurement_concept_id int,
							concept_name varchar,
							value_as_number numeric
							);			
					*/		
					
					--insert into htn_220119_step_21_covariate_measurement_for_comparator_sensitivity
					select distinct s09.person_id, s09.t_c, s09.index_date, m.measurement_concept_id, c2.concept_name, m.value_as_number 
					from htn_220119_step_08_drug_sequence_changed_unchanged s09
					left join measurement m on s09.person_id = m.person_id and m.measurement_date between s09.index_date - interval '180 days' and s09.index_date
					left join concept c2 on m.measurement_concept_id = c2.concept_id 
					where t_c = 'unchanged'

					
					
					
					
					
--Step 22: tibble table for sensitivity analysis
					/*
					create table htn_220119_step_22_sensitivity_analysis
								(
								person_id bigserial,
								t_c varchar,
								index_year int, 
								age int,
								gender varchar,
								abnormal_ecg varchar,
								af varchar,
								amyloidosis varchar,
								angina_pectoris varchar,
								arrhythmia varchar,
								arteriosclerosis_obliterans varchar,
								bladder_disease varchar,
								cardiomyopathy varchar,
								cardiovascular_disease varchar,
								cerebrovascular_disease varchar,
								coronary_arteriosclerosis varchar,
								diabetes_mellitus varchar,
								diabetic_neuropathy varchar,
								dyslipidemia varchar,
								dyspnea varchar,
								eye_disease varchar,
								gi_disease varchar,
								heart_failure varchar,
								hyperuricemia varchar,
								hypotension varchar,
								hypothyroidism varchar,
								insomnia varchar,
								liver_disease varchar,
								mental_and_behavioural_disorder varchar,
								musculoskeletal_disease varchar,
								neoplasm varchar,
								nervous_system_disorder varchar,
								nstemi varchar,
								obesity varchar,
								prediabetes varchar,
								prostatic_hyperplasia varchar,
								pulmonary_disease varchar,
								renal_disease varchar,
								symptom varchar,
								thyroid_disease varchar,
								analgesic_drug varchar,
								antianginal_agent varchar,
								antianxiety_agent varchar,
								antiarrhythmic_agent varchar,
								antibacterials varchar,
								anticoagulant_drug varchar,
								anticonvulsant varchar,
								antidepressant varchar,
								antidiabetic_agent varchar,
								antigout_agent varchar,
								antihyperlipidemic_agent varchar,
								antithyroid_agent varchar,
								antiviral_agent varchar,
								gastrointestinal_agent varchar,
								hmg varchar,
								immunosuppressive_agent varchar,
								micturition_disorder_drug varchar,
								nsaids varchar,
								respiratory_system_drug varchar,
								sedatives varchar,
								sbp_pre_value int,
								dbp_pre_value int
								);	
								*/

								select * from htn_220119_step_23_sensitivity_analysis;


								--insert into htn_220119_step_23_sensitivity_analysis
								with factor_con as 
										(
										select 
													distinct person_id, 
													t_c, 
													case when (cov_cond = 'abnormal ecg') then 1 else 0 end as abnormal_ecg,
													case when (cov_cond = 'af') then 1 else 0 end as af,
													case when (cov_cond = 'amyloidosis') then 1 else 0 end as amyloidosis,
													case when (cov_cond = 'angina pectoris') then 1 else 0 end as angina_pectoris,
													case when (cov_cond = 'arrhythmia') then 1 else 0 end as arrhythmia,
													case when (cov_cond = 'arteriosclerosis obliterans') then 1 else 0 end as arteriosclerosis_obliterans,
													case when (cov_cond = 'bladder disease') then 1 else 0 end as bladder_disease,
													case when (cov_cond = 'cardiomyopathy') then 1 else 0 end as cardiomyopathy,
													case when (cov_cond = 'cardiovascular disease') then 1 else 0 end as cardiovascular_disease,
													case when (cov_cond = 'cerebrovascular disease') then 1 else 0 end as cerebrovascular_disease,
													case when (cov_cond = 'coronary arteriosclerosis') then 1 else 0 end as coronary_arteriosclerosis,
													case when (cov_cond = 'diabetes mellitus') then 1 else 0 end as diabetes_mellitus,
													case when (cov_cond = 'diabetic neuropathy') then 1 else 0 end as diabetic_neuropathy,
													case when (cov_cond = 'dyslipidemia') then 1 
															 when (meas = 'tot_chol' and value_as_number >=240) then 1
															 when (meas = 'hdl' and value_as_number <40) then 1
															 when (meas = 'ldl' and value_as_number >=160) then 1
															 when (meas = 'tg' and value_as_number >200) then 1 else 0 end as dyslipidemia,
													case when (cov_cond = 'dyspnea') then 1 else 0 end as dyspnea,
													case when (cov_cond = 'eye disease') then 1 else 0 end as eye_disease,
													case when (cov_cond = 'gi disease') then 1 else 0 end as gi_disease,
													case when (cov_cond = 'heart failure') then 1 else 0 end as heart_failure,
													case when (cov_cond = 'hyperuricemia') then 1 else 0 end as hyperuricemia,
													case when (cov_cond = 'hypotension') then 1 else 0 end as hypotension,
													case when (cov_cond = 'hypothyroidism') then 1 else 0 end as hypothyroidism,
													case when (cov_cond = 'insomnia') then 1 else 0 end as insomnia,
													case when (cov_cond = 'liver disease') then 1
															 when (meas = 'ast' and value_as_number >=120) then 1
															 when (meas = 'alt' and value_as_number >=120) then 1 else 0 end as liver_disease,
													case when (cov_cond = 'mental and behavioural disorder') then 1 else 0 end as mental_and_behavioural_disorder,
													case when (cov_cond = 'musculoskeletal disease') then 1 else 0 end as musculoskeletal_disease,
													case when (cov_cond = 'neoplasm') then 1 else 0 end as neoplasm,
													case when (cov_cond = 'nervous system disorder') then 1 else 0 end as nervous_system_disorder,
													case when (cov_cond = 'nstemi') then 1 else 0 end as nstemi,
													case when (cov_cond = 'obesity') then 1 else 0 end as obesity,
													case when (cov_cond = 'prediabetes') then 1 else 0 end as prediabetes,
													case when (cov_cond = 'prostatic hyperplasia') then 1 else 0 end as prostatic_hyperplasia,
													case when (cov_cond = 'pulmonary disease') then 1 else 0 end as pulmonary_disease,
													case when (cov_cond = 'renal disease') then 1
															 when (meas = 'creatinine' and value_as_number > 1.4) then 1
															 when (meas = 'gfr' and value_as_number < 60) then 1 else 0 end as renal_disease,
													case when (cov_cond = 'symptom') then 1 else 0 end as symptom,
													case when (cov_cond = 'thyroid_disease') then 1 else 0 end as thyroid_disease
										from (
													select distinct s09.person_id, s09.t_c, ct.cov_cond, s12.value_as_number, ct2.meas, p.gender_source_value 
													from htn_220119_step_08_drug_sequence_changed_unchanged s09
													left join public.htn_220119_step_16_covariate_conditions_for_target_sensitivity s10 on s09.person_id = s10.person_id 
													left join htn_220119_code_table ct on s10.condition_concept_id = ct.concept_id 
													left join htn_220119_step_18_covariate_measurement_for_target_sensitivity s12 on s09.person_id = s12.person_id 
													left join htn_220119_code_table ct2 on s12.measurement_concept_id = ct2.concept_id
													left join person p on s09.person_id = p.person_id 
													where ct.cov_cond != 'NULL'
																union all 
													select distinct s09.person_id, s09.t_c, ct.cov_cond, s15.value_as_number, ct2.meas, p.gender_source_value 
													from htn_220119_step_08_drug_sequence_changed_unchanged s09
													left join public.htn_220119_step_19_covariate_conditions_for_comparator_sensitivity s13 on s09.person_id = s13.person_id 
													left join htn_220119_code_table ct on s13.condition_concept_id = ct.concept_id 
													left join htn_220119_step_21_covariate_measurement_for_comparator_sensitivity s15 on s09.person_id = s15.person_id 
													left join htn_220119_code_table ct2 on s15.measurement_concept_id = ct2.concept_id
													left join person p on s09.person_id = p.person_id 
													where ct.cov_cond != 'NULL'													
													)a
										order by person_id
										),
										cov_drug as
										(
										select 
													distinct person_id, 
													t_c, 
													case when (cov_drug = 'analgesic drug') then 1 else 0 end as analgesic_drug,
													case when (cov_drug = 'antianginal agent') then 1 else 0 end as antianginal_agent,
													case when (cov_drug = 'antianxiety agent') then 1 else 0 end as antianxiety_agent,
													case when (cov_drug = 'antiarrhythmic agent') then 1 else 0 end as antiarrhythmic_agent,
													case when (cov_drug = 'antibacterial') then 1 else 0 end as antibacterial,
													case when (cov_drug = 'anticoagulant drug') then 1 else 0 end as anticoagulant_drug,
													case when (cov_drug = 'anticonvulsant') then 1 else 0 end as anticonvulsant,
													case when (cov_drug = 'antidepressant') then 1 else 0 end as antidepressant,
													case when (cov_drug = 'antidiabetic agent') then 1 else 0 end as antidiabetic_agent,
													case when (cov_drug = 'antigout agent') then 1 else 0 end as antigout_agent,
													case when (cov_drug = 'antihyperlipidemic agent') then 1 else 0 end as antihyperlipidemic_agent,
													case when (cov_drug = 'antithyroid agent') then 1 else 0 end as antithyroid_agent,
													case when (cov_drug = 'antiviral_agent') then 1 else 0 end as antiviral_agent,
													case when (cov_drug = 'gastrointestinal agent') then 1 else 0 end as gastrointestinal_agent,
													case when (cov_drug = 'hmg') then 1 else 0 end as hmg,
													case when (cov_drug = 'immunosuppressive agent') then 1 else 0 end as immunosuppressive_agent,
													case when (cov_drug = 'micturition disorder drug') then 1 else 0 end as micturition_disorder_drug,
													case when (cov_drug = 'nsaids') then 1 else 0 end as nsaids,
													case when (cov_drug = 'respiratory system drug') then 1 else 0 end as respiratory_system_drug,
													case when (cov_drug = 'sedatives') then 1 else 0 end as sedatives
										from					
													(
													select distinct s09.person_id, s09.t_c, ct.cov_drug
													from htn_220119_step_08_drug_sequence_changed_unchanged s09
													left join public.htn_220119_step_17_covariate_drug_for_target_sensitivity s11 on s09.person_id = s11.person_id 
													left join htn_220119_code_table ct on s11.drug_concept_id = ct.concept_id 
													where ct.cov_drug != 'NULL'		
																union all 
													select distinct s09.person_id, s09.t_c, ct.cov_drug
													from htn_220119_step_08_drug_sequence_changed_unchanged s09
													left join public.htn_220119_step_20_covariate_drug_for_comparator_sensitivity s14 on s09.person_id = s14.person_id 
													left join htn_220119_code_table ct on s14.drug_concept_id = ct.concept_id 
													where ct.cov_drug != 'NULL'															
													)b
										order by person_id
										)
								select 
											distinct s09.person_id, 
											s09.t_c,
											extract (year from s09.index_date) as index_year,  
											max (extract (year from s09.drug_exposure_start_date) - p.year_of_birth) over (partition by s09.person_id) as age, 
											p.gender_source_value as gender,
											max(abnormal_ecg) over (partition by s09.person_id) as abnormal_ecg,
											max(af) over (partition by s09.person_id) as af,
											max(amyloidosis) over (partition by s09.person_id) as amyloidosis,
											max(angina_pectoris) over (partition by s09.person_id) as angina_pectoris,
											max(arrhythmia) over (partition by s09.person_id) as arrhythmia,
											max(arteriosclerosis_obliterans) over (partition by s09.person_id) as arteriosclerosis_obliterans,
											max(bladder_disease) over (partition by s09.person_id) as bladder_disease,
											max(cardiomyopathy) over (partition by s09.person_id) as cardiomyopathy,
											max(cardiovascular_disease) over (partition by s09.person_id) as cardiovascular_disease,
											max(cerebrovascular_disease) over (partition by s09.person_id) as cerebrovascular_disease,
											max(coronary_arteriosclerosis) over (partition by s09.person_id) as coronary_arteriosclerosis,
											max(diabetes_mellitus) over (partition by s09.person_id) as diabetes_mellitus,
											max(diabetic_neuropathy) over (partition by s09.person_id) as diabetic_neuropathy,
											max(dyslipidemia) over (partition by s09.person_id) as dyslipidemia,
											max(dyspnea) over (partition by s09.person_id) as dyspnea,
											max(eye_disease) over (partition by s09.person_id) as eye_disease,
											max(gi_disease) over (partition by s09.person_id) as gi_disease,
											max(heart_failure) over (partition by s09.person_id) as heart_failure,
											max(hyperuricemia) over (partition by s09.person_id) as hyperuricemia,
											max(hypotension) over (partition by s09.person_id) as hypotension,
											max(hypothyroidism) over (partition by s09.person_id) as hypothyroidism,
											max(insomnia) over (partition by s09.person_id) as insomnia,
											max(liver_disease) over (partition by s09.person_id) as liver_disease,
											max(mental_and_behavioural_disorder) over (partition by s09.person_id) as mental_and_behavioural_disorder,
											max(musculoskeletal_disease) over (partition by s09.person_id) as musculoskeletal_disease,
											max(neoplasm) over (partition by s09.person_id) as neoplasm,
											max(nervous_system_disorder) over (partition by s09.person_id) as nervous_system_disorder,
											max(nstemi) over (partition by s09.person_id) as nstemi,
											max(obesity) over (partition by s09.person_id) as obesity,
											max(prediabetes) over (partition by s09.person_id) as prediabetes,
											max(prostatic_hyperplasia) over (partition by s09.person_id) as prostatic_hyperplasia,
											max(pulmonary_disease) over (partition by s09.person_id) as pulmonary_disease,
											max(renal_disease) over (partition by s09.person_id) as renal_disase,
											max(symptom) over (partition by s09.person_id) as symptom,
											max(thyroid_disease) over (partition by s09.person_id) as thyroid_disease,
											max(analgesic_drug) over (partition by s09.person_id) as analgesic_drug,
											max(antianginal_agent) over (partition by s09.person_id) as antianginal_agent,
											max(antianxiety_agent) over (partition by s09.person_id) as antianxiety_agent,
											max(antiarrhythmic_agent) over (partition by s09.person_id) as antiarrhythmic_agent,
											max(antibacterial) over (partition by s09.person_id) as antibacterial,								
											max(anticoagulant_drug) over (partition by s09.person_id) as anticoagulant_drug,
											max(anticonvulsant) over (partition by s09.person_id) as anticonvulsant,
											max(antidepressant) over (partition by s09.person_id) as antidepressant,
											max(antidiabetic_agent) over (partition by s09.person_id) as antidiabetic_agent,
											max(antigout_agent) over (partition by s09.person_id) as antigout_agent,
											max(antihyperlipidemic_agent) over (partition by s09.person_id) as antihyperlipidemic_agent,
											max(antithyroid_agent) over (partition by s09.person_id) as antithyroid_agent,
											max(antiviral_agent) over (partition by s09.person_id) as antiviral_agent,
											max(gastrointestinal_agent) over (partition by s09.person_id) as gastrointestinal_agent,
											max(hmg) over (partition by s09.person_id) as hmg,
											max(immunosuppressive_agent) over (partition by s09.person_id) as immunosuppressive_agent,
											max(micturition_disorder_drug) over (partition by s09.person_id) as micturition_disorder_drug,
											max(nsaids) over (partition by s09.person_id) as nsaids,
											max(respiratory_system_drug) over (partition by s09.person_id) as respiratory_system_drug,
											max(sedatives) over (partition by s09.person_id) as sedatives,
											s04.sbp_pre_value,
											s04.dbp_pre_value 
								from htn_220119_step_08_drug_sequence_changed_unchanged s09
								left join person p on s09.person_id = p.person_id 
								left join factor_con cc on s09.person_id = cc.person_id
								left join cov_drug cd on s09.person_id = cd.person_id
								left join public.htn_220119_step_06_pre_post_bp_record s04 on s09.person_id = s04.person_id
								where s09.person_id in (select person_id from htn_220119_step_24_ps_matched_id_5)
								order by person_id
					
--Step 24: PS_matched_tibble table for sensitivity analysis & logistic regression
