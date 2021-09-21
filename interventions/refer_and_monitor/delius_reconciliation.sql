WITH INTERVENTIONS_USER AS (
    SELECT USER_ID FROM DELIUS_APP_SCHEMA.USER_ u WHERE u.DISTINGUISHED_NAME LIKE 'InterventionsReferAndMonitorSystem'
),
RAM_CONTACT AS (
    SELECT c.NSI_ID,
           REGEXP_SUBSTR(TO_CHAR(dbms_lob.substr(c.NOTES,1000,1)),'\w+ \w+ \w+') CONTACT_NOTES,
           ol.CODE OFFICE_LOCATION,
           TO_CHAR(c.CONTACT_DATE,'YYYY-MM-DD ') || TO_CHAR(c.CONTACT_START_TIME,'HH24:MI:SS') CONTACT_START_TIME,
           CASE WHEN c.CONTACT_END_TIME IS NULL THEN NULL ELSE TO_CHAR(c.CONTACT_DATE,'YYYY-MM-DD ') || TO_CHAR(c.CONTACT_END_TIME,'HH24:MI:SS') END CONTACT_END_TIME,
           CASE WHEN rct.ATTENDANCE_CONTACT = 'Y' THEN c.CONTACT_ID ELSE NULL END DELIUS_APPOINTMENT_ID,
           c.ATTENDED,
           c.COMPLIED,
           CASE WHEN iu3.USER_ID IS NULL THEN 'N' ELSE 'Y' END CONTACT_CREATED_BY_RAM,
           CASE WHEN iu4.USER_ID IS NULL THEN 'N' ELSE 'Y' END CONTACT_LAST_UPDATED_BY_RAM
    FROM DELIUS_APP_SCHEMA.CONTACT c
    INNER JOIN DELIUS_APP_SCHEMA.R_CONTACT_TYPE rct ON rct.CONTACT_TYPE_ID = c.CONTACT_TYPE_ID AND rct.CODE LIKE 'CRS%' -- ignore delius generated ones
    LEFT JOIN DELIUS_APP_SCHEMA.OFFICE_LOCATION ol ON ol.OFFICE_LOCATION_ID = c.OFFICE_LOCATION_ID -- no location for notification
    LEFT JOIN DELIUS_APP_SCHEMA.R_CONTACT_OUTCOME_TYPE rcto ON rcto.CONTACT_OUTCOME_TYPE_ID = c.CONTACT_OUTCOME_TYPE_ID
    LEFT JOIN INTERVENTIONS_USER iu3 ON iu3.USER_ID = c.CREATED_BY_USER_ID
    LEFT JOIN INTERVENTIONS_USER iu4 ON iu4.USER_ID = c.LAST_UPDATED_USER_ID
)
SELECT TO_CHAR(SUBSTR(REGEXP_SUBSTR(dbms_lob.substr(nsi.NOTES,1000,1),'urn:hmpps:interventions-referral:.*'),34)) REFERRAL_ID,
       o.CRN SERVICE_USERCRN,
       TO_CHAR(SUBSTR(REGEXP_SUBSTR(dbms_lob.substr(nsi.NOTES,1000,1),' Referral \w+ '),10,18)) REFERENCE_NUMBER,
       rnt.DESCRIPTION NAME,
       nsi.EVENT_ID RELEVANT_SENTENCE_ID,
       TO_CHAR(nsi.REFERRAL_DATE,'YYYY-MM-DD') REFERRAL_START,
       TO_CHAR(nsi.NSI_STATUS_DATE,'YYYY-MM-DD HH24:MI:SS') STATUS_AT,
       rns.DESCRIPTION STATUS,
       rsrl.CODE_DESCRIPTION OUTCOME,
       CASE WHEN iu2.USER_ID IS NULL THEN 'N' ELSE 'Y' END REFERRAL_LAST_UPDATED_BY_RAM,
       rc.CONTACT_NOTES,
       rc.OFFICE_LOCATION,
       rc.CONTACT_START_TIME,
       rc.CONTACT_END_TIME,
       rc.DELIUS_APPOINTMENT_ID,
       rc.ATTENDED,
       rc.COMPLIED,
       CASE WHEN rc.CONTACT_CREATED_BY_RAM IS NULL THEN 'Y' ELSE rc.CONTACT_CREATED_BY_RAM END CONTACT_CREATED_BY_RAM,
       CASE WHEN rc.CONTACT_LAST_UPDATED_BY_RAM IS NULL THEN 'Y' ELSE rc.CONTACT_LAST_UPDATED_BY_RAM END CONTACT_LAST_UPDATED_BY_RAM
FROM DELIUS_APP_SCHEMA.NSI nsi
INNER JOIN DELIUS_APP_SCHEMA.R_NSI_TYPE rnt ON rnt.NSI_TYPE_ID = nsi.NSI_TYPE_ID AND rnt.CODE LIKE 'CRS0%' -- only R&M nsi types
INNER JOIN DELIUS_APP_SCHEMA.R_NSI_STATUS rns ON rns.NSI_STATUS_ID = nsi.NSI_STATUS_ID
LEFT JOIN DELIUS_APP_SCHEMA.R_STANDARD_REFERENCE_LIST rsrl ON rsrl.STANDARD_REFERENCE_LIST_ID = nsi.NSI_OUTCOME_ID
INNER JOIN DELIUS_APP_SCHEMA.OFFENDER o ON o.OFFENDER_ID = nsi.OFFENDER_ID
LEFT JOIN INTERVENTIONS_USER iu2 ON iu2.USER_ID = nsi.LAST_UPDATED_USER_ID
LEFT JOIN RAM_CONTACT rc on rc.NSI_ID = nsi.NSI_ID -- only R&M contacts
WHERE TO_CHAR(dbms_lob.substr(nsi.NOTES, 1000,1)) LIKE 'urn:hmpps:interventions-referral:%' -- only R&M referrals
--- AND nsi.REFERRAL_DATE = TO_DATE('2021-07-10', 'YYYY-MM-DD')
AND nsi.SOFT_DELETED = 0
-- AND o.CRN = 'E379156'
ORDER BY nsi.REFERRAL_DATE,
         o.CRN,
         nsi.NSI_STATUS_DATE,
         rc.CONTACT_START_TIME