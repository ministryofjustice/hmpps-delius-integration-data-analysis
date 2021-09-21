import csv
import re
from pprint import pformat
import logging
from argparse import ArgumentParser
from dictdiffer import diff


DELIUS_FILE = ""
RAM_FILE = ""


def parse_csv(file):
    """Create a dict of CSV headings to values"""
    with open(file) as csv_file:

        # Parse CSV file
        parsed = csv.reader(csv_file)

        # Create a dict of CSV heading to values
        headings = next(parsed)
        rows = (dict(zip(headings, line)) for line in parsed)

        # Create a dict with a unique key for each specific contact
        keyed = {}
        for row in rows:
            k = row["REFERRAL_ID"] + row["CONTACT_START_TIME"] + row["CONTACT_NOTES"]
            keyed[k] = row

        return keyed


def missing_feedback_in_delius(_, raw_diff):
    """Issue #3: R&M feedback not reflected in Delius"""
    identifier = ""
    attended = [item for item in raw_diff if item[1] == "ATTENDED"]
    complied = [item for item in raw_diff if item[1] == "COMPLIED"]
    if (len(attended) > 0 and attended[0][2][1] == "") or (
        len(complied) > 0 and complied[0][2][1] == ""
    ):
        identifier = "DELIUS_MISSING_ATTENDED_COMPLIED_FEEDBACK"
    return identifier


def missing_feedback_in_ram(_, raw_diff):
    """Issue #4: R&M feedback different in Delius"""
    identifier = ""
    attended = [item for item in raw_diff if item[1] == "ATTENDED"]
    complied = [item for item in raw_diff if item[1] == "COMPLIED"]
    if (len(attended) > 0 and attended[0][2][0] == "") or (
        len(complied) > 0 and complied[0][2][0] == ""
    ):
        identifier = "R&M_MISSING_ATTENDED_COMPLIED_FEEDBACK"
    return identifier


seen_referrals = {}


def nsi_updated_in_delius(referral_id, diffs):
    """Issue #5: Referral Updated manually in Delius"""
    identifier = ""
    interesting = [item for item in diffs if item[1] == "REFERRAL_LAST_UPDATED_BY_RAM"]
    if interesting:
        if referral_id not in seen_referrals:
            seen_referrals[referral_id] = True
            identifier = "NSI_LAST_UPDATED_MANUALLY_IN_DELIUS"
        else:
            identifier = "ALREADY_ACCOUNTED_FOR"

    return identifier


def missing_status_in_delius(_, diffs):
    """Identify when status change not reflected in Delius"""
    identifier = ""
    status = [item for item in diffs if item[1] == "STATUS"]
    if len(status) > 0:
        identifier = "NSI_STATUS_NOT_UPDATED_IN_DELIUS"
    return identifier


def appointment_updated_in_delius(_, diffs):
    """Identify when appointment manually updated in Delius"""
    identifier = ""
    interesting = [item for item in diffs if item[1] == "CONTACT_LAST_UPDATED_BY_RAM"]
    if interesting:
        identifier = "APPOINTMENT_LAST_UPDATED_MANUALLY_IN_DELIUS"
    return identifier


def appointment_location_updated_in_delius(_, diffs):
    """Identify when appointment location manually updated in Delius"""
    identifier = ""
    interesting = [item for item in diffs if item[1] == "OFFICE_LOCATION"]
    if interesting:
        identifier = "APPOINTMENT_LOCATION_UPDATED_MANUALLY_IN_DELIUS"
    return identifier


def appointment_duplication(_, diffs):
    """Identify when appointment has been duplicated"""
    identifier = ""
    interesting = [item for item in diffs if item[1] == "DELIUS_APPOINTMENT_ID"]
    if interesting:
        identifier = "TWO_APPOINTMENTS_AT_SAME_TIME"
    return identifier


def appointment_end_time_different(_, diffs):
    """Identify when appointment end time has been updated"""
    identifier = ""
    interesting = [item for item in diffs if item[1] == "CONTACT_END_TIME"]
    if interesting:
        identifier = "APPOINTMENT_END_TIME_DIFFERENT"
    return identifier


def incorrect_deletions_for_multiple_nsi(_, diffs):
    """Identify when the wrong NSI has been deleted from Delius"""
    identifier = ""
    interesting = [item for item in diffs if item[1] == "REFERENCE_NUMBER"]
    if interesting:
        identifier = "WRONG_DUPLICATE_NSI_DELETED_FROM_DELIUS?"
    return identifier


def classify_problems(referral_id, diffs):
    """Apply identification functions to each diff"""
    funcs = [
        nsi_updated_in_delius,
        appointment_updated_in_delius,
        missing_status_in_delius,
        missing_feedback_in_ram,
        missing_feedback_in_delius,
        appointment_location_updated_in_delius,
        appointment_duplication,
        appointment_end_time_different,
        incorrect_deletions_for_multiple_nsi,
    ]

    for func in funcs:
        identifier = func(referral_id, diffs)
        if identifier:
            return identifier

    logging.debug("%s", diffs)

    return "UNCLASSIFIED"


def classify_missing_appointments(row):
    """Identify why an appointment has been deleted from Delius"""
    identifier = ""
    if (row["STATUS"]) == "Completed":
        identifier = "DELETED_DUE_TO_COMPLETION_IN_R&M"
    else:
        identifier = "DELETED_DUE_TO_COMPLETION_IN_DELIUS"

    return identifier


if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("--log")
    args = parser.parse_args()
    args.log = args.log or "INFO"

    logging.basicConfig(level=getattr(logging, args.log.upper()))

    delius = parse_csv(DELIUS_FILE)
    ram = parse_csv(RAM_FILE)

    missing_from_delius_contact_type = {}
    problem_identifier = {}
    stats = {}
    delivery_appt = {}

    for key, _ in ram.items():
        ram_row = ram[key]

        if key in delius:
            # Save out the CSV row for diffing
            delius_row = delius[key]
        else:
            # These are not in Delius at all
            stats["MISSING"] = stats.setdefault("MISSING", 0).__add__(1)

            # What kind of activity is it?
            contact_type_key = ram_row["CONTACT_NOTES"]

            # Contacts that differ as the database dumps were not made
            # at the same time
            if not contact_type_key:
                contact_type_key = "DATA_EXPORT_SYNCHRONISATION"

            # Appointment contacts may be missing as future appointments
            # with no outcome are deleted when a referral is completed
            if re.match(".*Appointment", contact_type_key):
                # Classify missing appointments
                PROBLEM_ID = classify_missing_appointments(ram_row)

                # Count missing appointment issues
                delivery_appt[PROBLEM_ID] = delivery_appt.setdefault(
                    PROBLEM_ID, 0
                ).__add__(1)

            # Count contact types where row is missing from Delius
            if contact_type_key in missing_from_delius_contact_type:
                missing_from_delius_contact_type[contact_type_key] += 1
            else:
                missing_from_delius_contact_type[contact_type_key] = 1

            # Set the Delius row to be the R&M row - i.e. ignore the diff
            delius_row = ram[key]

        # Diff the two CSV rows
        raw_list = list(
            diff(
                ram_row,
                delius_row,
                ignore=["NAME", "OUTCOME", "STATUS_AT"],
            )
        )

        if raw_list != []:
            # These are different between R&M and Delius - log so we can eyeball
            # logging.debug(
            #     "%s | %s | %s", key, ram[key]["SERVICE_USERCRN"], ram[key]["NAME"]
            # )
            # logging.debug("%s\n", raw_list)

            # Classify and count the differences
            stats["DIFFERENT"] = stats.setdefault("DIFFERENT", 0).__add__(1)

            # Classify problems
            PROBLEM_ID = classify_problems(ram[key]["REFERRAL_ID"], raw_list)

            # Count identified problems
            if PROBLEM_ID != "ALREADY_ACCOUNTED_FOR":
                problem_identifier[PROBLEM_ID] = problem_identifier.setdefault(
                    PROBLEM_ID, 0
                ).__add__(1)

        else:
            # These are all gravy - whoo!
            stats["MATCHING"] = stats.setdefault("MATCHING", 0).__add__(1)

    # Remove missing from the matching total
    stats["MATCHING"] = stats["MATCHING"] - stats["MISSING"]

    # Print out some bits and bobs
    logging.info("\nStats:\n %s\n", pformat(stats))

    logging.info("\nDifferences:\n %s\n", pformat(problem_identifier))

    logging.info(
        "\nContacts missing from Delius:\n %s\n",
        pformat(missing_from_delius_contact_type),
    )

    logging.info("\nAppointments missing from Delius:\n %s\n", pformat(delivery_appt))
