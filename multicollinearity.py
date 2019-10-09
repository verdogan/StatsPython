from scipy.stats import pearsonr
import numpy as np
from connection import connect, currtime
import argparse
import json
import logging
from checkprint import checkprint
from checkwarns import checkwarns


checkwarns()

writelog = False

if not writelog:
    log = logging.getLogger()
    log.setLevel(logging.CRITICAL)

test = False

def main(org_id, survey_id, variable_ids):
    if org_id == 0 or survey_id == 0 or variable_ids == "":
        exit(1)

    variable_ids = variable_ids.split(",")

    cnx = connect()
    now = currtime()

    data_raw = []

    for variable_id in variable_ids:
        cursor = cnx.cursor(dictionary=True, buffered=True)
        try:
            cursor.callproc('GetVariableDataF', [variable_id, org_id, survey_id, 'FALSE', 'NULL'])
        except:
            cursor_err = cnx.cursor()
            # cursor_err.execute(sperror("Variables can't be retrieved", analysis_id))
            cnx.commit()
            cursor_err.close()
            cnx.close()
            print("-")
            exit(1)
        for i in cursor.stored_results():
            data_raw.append(i.fetchall())
        cursor.close()

    data = []
    for variable in data_raw:
        data_points = []
        for point in variable:
            try:
                to_be_appended = float(point[0])
            except:
                to_be_appended = 0.0
            data_points.append(to_be_appended)
        data.append(data_points)

    result = []
    for i in range(0, len(data)):
        for j in range(i, len(data)):
            if i != j:
                try:
                    corr, p = pearsonr(data[i], data[j])
                    if np.isnan(corr):
                        corr = 0.00
                except:
                    checkprint("ERR_IND")
                    exit(1)
                result.append({"v1": variable_ids[i], "v2": variable_ids[j], "c": round(corr, 2)})

    result = json.dumps(result)
    print(result)

    cnx.close()

    return result


if __name__ == "__main__":
    if test:
        org_id = 1
        survey_id = 11
        variable_ids = "205, 232"
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument('--org', '-o', help="", type=int, default=0)
        parser.add_argument('--survey', '-s', help="", type=int, default=0)
        parser.add_argument('--variables', '-v', help="", type=str, default="")
        args = parser.parse_args()
        org_id = args.org
        survey_id = args.survey
        variable_ids = args.variables

    main(org_id, survey_id, variable_ids)