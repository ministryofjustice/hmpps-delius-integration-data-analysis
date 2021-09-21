psql --host=localhost  --port=5432 --dbname=dbname --username=username --password --command "\copy (
with sent_referral as (
	select 	r.id referral_id,
	        r.service_usercrn,
	        r.reference_number,
	        'CRS ' || ct.name \"name\",
			r.relevant_sentence_id,
			'urn:hmpps:interventions-referral:'||r.id||'\\n'||
			'Referral Sent for '||ct.name||' Referral '||r.reference_number||
			' with Prime Provider '||sp.name notes,
			date(r.sent_at) referral_start,
	        end_requested_at,
	        concluded_at,
			to_char(greatest(r.sent_at, end_requested_at, concluded_at)
				 at time zone 'Europe/London', 'YYYY-MM-DD HH24:MI:SS') status_at,
	        eosr.submitted_at eosr_submitted_at
	from referral r
	inner join intervention i on i.id = r.intervention_id
	inner join dynamic_framework_contract dfc on dfc.id = i.dynamic_framework_contract_id
	inner join contract_type ct on ct.id = dfc.contract_type_id
	inner join service_provider sp on sp.id = dfc.prime_provider_id
	left join end_of_service_report eosr on eosr.referral_id = r.id
),
referral_contact as (
	select 	referral_id,
	        'CRS Supplier Assessment' contact_type,
			'Supplier Assessment Appointment' contact_notes,
	        'CRSEXTL' office_location,
			to_char(appointment_time at time zone 'Europe/London', 'YYYY-MM-DD HH24:MI:SS') contact_start_time,
			to_char((appointment_time + (duration_in_minutes::text||' minute')::INTERVAL)
					at time zone 'Europe/London', 'YYYY-MM-DD HH24:MI:ss') contact_end_time,
	        a.delius_appointment_id,
	        case when a.attended in ('LATE','YES') then 'Y'
	             when a.attended in ('NO') then 'N'
                 else null end attended,
	        a.notifyppof_attendance_behaviour,
	        case when a.notifyppof_attendance_behaviour = true then 'N'
	             when a.notifyppof_attendance_behaviour = false then 'Y'
	             else null end complied
	from appointment a
	inner join supplier_assessment_appointment saa on saa.appointment_id = a.id
	union
	select 	referral_id,
			'CRS Notes' contact_type,
			'Action Plan Submitted' contact_notes,
	        null office_location,
			to_char(submitted_at at time zone 'Europe/London', 'YYYY-MM-DD HH24:MI:SS') contact_start_time,
			null end_time,
	        null delius_appointment_id,
	        null attended,
	        null notifyppof_attendance_behaviour,
	        null complied
	from action_plan ap
	where ap.submitted_at is not null
	union
	select 	referral_id,
			'CRS Notes' contact_type,
			'Action Plan Approved' contact_notes,
	        null office_location,
			to_char(approved_at at time zone 'Europe/London', 'YYYY-MM-DD HH24:MI:SS') contact_start_time,
			null end_time,
	        null delius_appointment_id,
	        null attended,
	        null notifyppof_attendance_behaviour,
            null complied
	from action_plan ap
	where ap.approved_at is not null
	union
	select 	referral_id,
	        'CRS Service Delivery' contact_type,
			'Service Delivery Appointment' contact_notes,
	        'CRSEXTL' office_location,
			to_char(appointment_time at time zone 'Europe/London', 'YYYY-MM-DD HH24:MI:SS') contact_start_time,
			to_char((appointment_time + (duration_in_minutes::text||' minute')::INTERVAL)
					at time zone 'Europe/London', 'YYYY-MM-DD HH24:MI:SS') contact_end_time,
	        a.delius_appointment_id,
	        case when a.attended in ('LATE','YES') then 'Y' else null end attended,
	        a.notifyppof_attendance_behaviour,
	        case when a.notifyppof_attendance_behaviour = true then 'N'
	             when a.notifyppof_attendance_behaviour = false then 'Y'
	             else null end complied
	from appointment a
	inner join action_plan_session_appointment apsa on apsa.appointment_id = a.id
	union
	select 	referral_id,
			'CRS Notes' contact_type,
			'End of Service' contact_notes,
	        null office_location,
			to_char(submitted_at at time zone 'Europe/London', 'YYYY-MM-DD HH24:MI:SS') contact_start_time,
			null end_time,
	        null delius_appointment_id,
	        null attended,
	        null notifyppof_attendance_behaviour,
	        null complied
	from end_of_service_report
),
latest_action_plan as (
	select * from (
		select r.id referral_id,
			   ap.id action_plan_id,
		       ap.number_of_sessions,
			   row_number() over (partition by r.id order by approved_at, submitted_at desc nulls last) rank
		from referral r
		inner join action_plan ap on ap.referral_id = r.id
	) action_plans
	where action_plans.rank = 1
),
attempted_sessions as (
	select 	aps.action_plan_id,
	        sum(case when a.attended is not null and a.appointment_feedback_submitted_at is not null then 1
				else 0 end) total_attempted
	from appointment a
	inner join action_plan_session_appointment apsa on apsa.appointment_id = a.id
	inner join action_plan_session aps on aps.id = apsa.action_plan_session_id
	group by aps.action_plan_id
)
select  sr.referral_id,
	    sr.service_usercrn,
	    sr.reference_number,
	    sr.name,
		sr.relevant_sentence_id,
		sr.referral_start,
	    sr.status_at,
        case when concluded_at is not null
                then 'Completed'
		     else 'In Progress' end status,
        case when concluded_at is not null and coalesce(total_attempted,0) = 0
				then 'CRS Referral Cancelled'
		     when concluded_at is not null and coalesce(total_attempted,0) < lap.number_of_sessions and eosr_submitted_at is not null
			 	then 'CRS Referral Ended'
		     when concluded_at is not null and coalesce(total_attempted,0) >= lap.number_of_sessions and eosr_submitted_at is not null
			 	then 'CRS Service Completed'
		     else null end outcome,
		'Y' referral_last_updated_by_ram,
		rc.contact_notes,
	    rc.office_location,
		rc.contact_start_time,
		rc.contact_end_time,
		rc.delius_appointment_id,
	    rc.attended,
	    rc.complied,
		'Y' contact_created_by_ram,
		'Y' contact_last_updated_by_ram
from sent_referral sr
left join referral_contact rc on rc.referral_id = sr.referral_id
left join latest_action_plan lap on lap.referral_id = sr.referral_id
left join attempted_sessions ats on ats.action_plan_id = lap.action_plan_id
order by sr.referral_start,
         sr.service_usercrn,
		 sr.status_at,
		 rc.contact_start_time
) to 'output.csv' WITH (FORMAT CSV, HEADER TRUE, FORCE_QUOTE *);"