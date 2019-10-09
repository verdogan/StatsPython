import numpy as np
from scipy.stats import pearsonr, pointbiserialr
from connection import connect, currtime
from sperror import sperror
import sys
import logging
from checkwarns import checkwarns
import argparse
import json


checkwarns()

writelog = False

if not writelog:
    log = logging.getLogger()
    log.setLevel(logging.CRITICAL)

test = False

cnx = connect()
now = currtime()

# binumerical correlation
def twocorr(x, y):
    try:
        corr, p = pearsonr(x, y)
    except:
        cursor_err = cnx.cursor()
        # cursor_err.execute(sperror("Correlation can't be done", analysis_id))
        cursor_err.execute(sperror("Correlation can't be done", 1))
        cnx.commit()
        cursor_err.close()
        cnx.close()
        exit(1)

    return corr, p


def ro(analysis_id):
    # terminate if analysis id is invalid
    try:
        analysis_id = int(analysis_id)
    except:
        exit(1)

    if analysis_id < 1:
        exit(1)

    # select given analysis id
    cursor1 = cnx.cursor(dictionary=True)
    try:
        cursor1.execute("SELECT * FROM analysis WHERE id = {current_id}".format(current_id = analysis_id))
    except:
        exit(1)
    result = cursor1.fetchall()
    cursor1.close()

    # terminate if the analysis type is not correlation
    if result[0]["analysis_type"] != "CORRELATION":
        cursor2 = cnx.cursor()
        cursor2.execute(sperror("Analysis is not Correlational", analysis_id))
        cnx.commit()
        cursor2.close()
        cnx.close()
        exit(1)

    # start the analysis
    cursor3 = cnx.cursor(dictionary=True)
    try:
        cursor3.execute("""UPDATE analysis SET analysis_status = 'running',
                                error = NULL,
                                modified = '{current_time}' 
                                WHERE id = {current_id}""".format(current_time=now, current_id=analysis_id))
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("Analysis couldn't start", analysis_id))
        cnx.commit()
        cursor_err.close()
        cnx.close()
        exit(1)
    cnx.commit()
    cursor3.close()

    # remove old results
    cursor4 = cnx.cursor()
    try:
        cursor4.execute("""DELETE FROM correlation_analysis_results 
                            WHERE analysis_id = {current_id}""".format(current_id=analysis_id))
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("Can't access or modify analysis results", analysis_id))
        cnx.commit()
        cursor_err.close()
        cnx.close()
        exit(1)
    cnx.commit()
    cursor4.close()

    # fetch variables
    cursor5 = cnx.cursor(dictionary=True, buffered=True)
    ro_id = result[0]["ro_id"]
    try:
        cursor5.execute("""SELECT var_id AS id, var_type AS type, var_subtype AS subtype FROM ro_vars 
                        INNER JOIN var_list ON var_list.var_id = ro_vars.variable_id 
                        WHERE ro_vars.ro_id = {current_ro_id}""".format(current_ro_id=ro_id))
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("A variable can't be retrieved", analysis_id))
        cnx.commit()
        cursor_err.close()
        cnx.close()
        exit(1)
    variables = cursor5.fetchall()
    cursor5.close()

    # get options for categorical variables
    options = []
    for variable in variables:
        # checkprint(variable)
        cursor8 = cnx.cursor(dictionary=True, buffered=True)
        if True:
            try:
                cursor8.callproc('GetVariableOptions', [variable["id"]])
            except:
                cursor_err = cnx.cursor()
                cursor_err.execute(sperror("Variables can't be retrieved", analysis_id))
                cnx.commit()
                cursor_err.close()
                cnx.close()
                exit(1)
            for i in cursor8.stored_results():
                options.append(i.fetchall())
        cursor8.close()

    # checkprint(options)

    # fetch data points for each variable
    data_raw = []
    j = 0
    for variable in variables:
        cursor8 = cnx.cursor(dictionary=True, buffered=True)
        try:
            cursor8.callproc('GetVariableDataF', [variable["id"], result[0]["org_id"], result[0]["survey_id"],
                                                  result[0]["ro_id"], 0, ''])
        except:
            cursor_err = cnx.cursor()
            cursor_err.execute(sperror("Variables can't be retrieved", analysis_id))
            cnx.commit()
            cursor_err.close()
            cnx.close()
            exit(1)
        for i in cursor8.stored_results():
            data_raw.append(i.fetchall())
        j += 1
        cursor8.close()
    for var1 in data_raw:
        for var2 in data_raw:
            if len(var1) != len(var2):
                cursor_err = cnx.cursor()
                cursor_err.execute(sperror("Variables are not equal in length", analysis_id))
                cnx.commit()
                cursor_err.close()
                cnx.close()
                exit(1)
    data_with_nones = []
    for variable in data_raw:
        row = []
        for value in variable:
            if value[0] is not None:
                row.append(value[0])
            else:
                row.append(None)
        data_with_nones.append(row)
    data = data_with_nones
    # print(data)
    # print(len(data))

    added = []
    for i in range(0, len(variables)):
        for j in range(0, len(variables)):
            # check if second variable is numerical
            if variables[j]['subtype'] in ['decimal', 'numeric', 'scalar', 'labeled_scalar']:  # is j numerical? if not, continue
                tempvar_i = []
                tempvar_j = []
                results = []
                for k in range(0, len(data[i])):
                    if data[i][k] is not None and data[j][k] is not None:
                        tempvar_i.append(data[i][k])
                        tempvar_j.append(data[j][k])
                print(len(tempvar_i))
                print(len(tempvar_j))
                if tempvar_i[1:] == tempvar_i[:-1] or tempvar_j[1:] == tempvar_j[:-1]:
                    corr, p = 0.0, 1.0
                    results.append([corr, p])
                # check to see if two arrays are identical, like [1,2,3,4,5], [1,2,3,4,5]
                elif tempvar_i == tempvar_j:
                    corr, p = 1.0, 0.0
                    results.append([corr, p])
                else:
                    # binumerical
                    if variables[i]['subtype'] in ['decimal', 'numeric', 'scalar', 'labeled_scalar']:  # numerical-numerical
                        var_i = [float(x) for x in tempvar_i]
                        var_j = [float(x) for x in tempvar_j]
                        corr, p = twocorr(var_i, var_j)
                        print(variables[i])
                        print(variables[j])
                        print(corr)
                        if np.isnan(corr) or np.isnan(p):
                            corr, p = 0.00, 1.00
                        corr = round(corr, 2)
                        p = round(p, 2)
                        results.append([corr, p])
                    # categorical-numerical
                    elif variables[i]['subtype'] in ['single_choice', 'multiple_choice', 'text_scalar']:  # categorical-numerical
                        var_j = [float(x) for x in tempvar_j]
                        for opt in range(1, len(options[i]) + 1):
                            binary = []
                            for point in tempvar_j:
                                if point is None:
                                    binary.append(0)
                                elif str(opt) in str(point).split(','):
                                    binary.append(1)
                                else:
                                    binary.append(0)
                            # add the result of test to results array
                            print(variables[i])
                            print(variables[j])
                            print(options[i])
                            print(len(data[i]))
                            print(len(var_j))
                            corr, p = pointbiserialr(binary, var_j)
                            if np.isnan(corr) or np.isnan(p):
                                corr, p = 0.00, 1.00
                            print(corr)
                            corr = round(corr, 2)
                            p = round(p, 2)
                            results.append([corr, p])
                    else:
                        exit(1)
                if variables[i]['subtype'] in ['decimal', 'numeric', 'scalar', 'labeled_scalar']:
                    option_end = 2
                else:
                    option_end = len(options[i]) + 1
                # checkprint("---")
                for k in range(1, option_end):  # number of options + 1 for ith variable for categorical, 2 for numerical
                    try:
                        if variables[i]['subtype'] in ['decimal', 'numeric', 'scalar', 'labeled_scalar']:
                            i_option = 'NULL'
                            result_count = 0
                        else:
                            # i_option = k
                            i_option, i_label = options[i][k-1]
                            result_count = k - 1
                        # checkprint(result_count)
                        # checkprint(results)
                        cursor91 = cnx.cursor()
                        cursor92 = cnx.cursor()
                        if len(results) > result_count:
                            cursor91.execute("""INSERT INTO correlation_analysis_results SET analysis_id = {current_id}, 
                                                var1 = {var1}, var2 = {var2}, var1_option = {var1_option}, 
                                                corr_value = {corr_value}, p_value = {p_value}""".format(
                                                current_id=analysis_id, var1=variables[i]["id"], var2=variables[j]["id"],
                                                var1_option=i_option,
                                                corr_value=results[result_count][0],
                                                p_value=results[result_count][1]))
                        else:
                            cursor91.execute("""INSERT INTO correlation_analysis_results SET analysis_id = {current_id}, 
                                                                        var1 = {var1}, var2 = {var2}, var1_option = {var1_option}, 
                                                                        corr_value = {corr_value}, p_value = {p_value}""".format(
                                current_id=analysis_id, var1=variables[i]["id"], var2=variables[j]["id"],
                                var1_option=i_option,
                                corr_value=0.0,
                                p_value=1.0))
                        added.append((i, j))
                    except:
                        cursor_err = cnx.cursor()
                        cursor_err.execute(sperror("Correlation results can't be written to database", analysis_id))
                        cnx.commit()
                        cursor_err.close()
                        cnx.close()
                        exit(1)
                    cnx.commit()
                    cursor91.close()
                    cursor92.close()

    cursor10 = cnx.cursor(dictionary=True)
    try:
        cursor10.execute("""UPDATE analysis SET analysis_status = 'done',
                                error = NULL,
                                modified = '{current_time}' 
                                WHERE id = {current_id}""".format(current_time=now, current_id=analysis_id))
    except:
        exit(1)
    cnx.commit()
    cursor10.close()
    cnx.close()
    exit(0)


def suggest(org_id, survey_id, variable_ids, threshold, count):
    variable_ids = variable_ids.split(',')

    cursor = cnx.cursor(buffered=True, dictionary=True)
    try:
        query = """SELECT var_id AS id, var_type AS type, var_subtype AS subtype, var_name AS name FROM var_list 
                   WHERE org_id = {org_id} AND survey_id = {survey_id} 
                   AND var_subtype IN ('single_choice','multiple_choice','numeric',
                   'scalar','text_scalar','labeled_scalar','decimal');"""
        query = query.format(org_id=org_id, survey_id=survey_id)
        cursor.execute(query)
    except:
        exit(1)
    allvarlist = cursor.fetchall()
    # print(allvarlist)
    allvarids = [str(var['id']) for var in allvarlist]
    # print(allvarids)
    cursor.close()

    options = []
    # print(allvarlist)
    for var in allvarlist:
        cursor = cnx.cursor(dictionary=True, buffered=True)
        cursor.callproc('GetVariableOptions', [var['id']])
        for i in cursor.stored_results():
            options.append(i.fetchall())
        cursor.close()
    # print(options)

    options_input_vars = []
    for var_id in variable_ids:
        cursor = cnx.cursor(dictionary=True, buffered=True)
        cursor.callproc('GetVariableOptions', [var_id])
        for i in cursor.stored_results():
            options_input_vars.append(i.fetchall())
        cursor.close()
    # print(options_input_vars)

    cursor = cnx.cursor(dictionary=True, buffered=True)
    cursor.callproc('GetSurveyData', [org_id, survey_id, 1, None, None])
    for i in cursor.stored_results():
        columns = i.column_names
        dataset = i.fetchall()
    # print(dataset)

    ### step 4 ###

    cleaned_dataset = []
    for varid in allvarids:
        col = []
        indx = columns.index(varid)
        for row in dataset:
            col.append(row[indx])
        cleaned_dataset.append(col)

    data_chosen = []
    for varid in variable_ids:
        col = []
        indx = columns.index(varid)
        for row in dataset:
            col.append(row[indx])
        data_chosen.append(col)

    correlations = {}
    for var_id in allvarids:
        correlations[var_id] = []
    for i in range(len(variable_ids)):
        # print(variable_ids[i])
        var1_type = [var['subtype'] for var in allvarlist if var['id'] == int(variable_ids[i])]
        var1_type = var1_type[0]
        # print(var1_type)
        if var1_type in ['decimal', 'numeric', 'scalar', 'labeled_scalar']:
            for j in range(len(allvarids)):
                if variable_ids[i] == allvarids[j]:
                    continue
                elif allvarids[j] in variable_ids:
                    continue
                else:
                    var2_type = allvarlist[j]['subtype']
                    if var2_type in ['decimal', 'numeric', 'scalar', 'labeled_scalar']:
                        list1 = [float(val) if val is not None else 0.0 for val in data_chosen[i]]
                        list2 = [float(val) if val is not None else 0.0 for val in cleaned_dataset[j]]
                        corr, p = pearsonr(list1, list2)
                        if p < 0.5:
                            correlations[allvarids[j]].append(abs(corr))
                    elif var2_type in ['single_choice', 'multiple_choice']:
                        corrs = []
                        list1 = [float(val) if val is not None else 0.0 for val in data_chosen[i]]
                        thisopts = options[j]
                        for option in thisopts:
                            binary = []
                            for point in cleaned_dataset[j]:
                                if point is None:
                                    binary.append(0)
                                elif str(point).find(str(option[0])) != -1:
                                    binary.append(1)
                                else:
                                    binary.append(0)
                            corr, p = pointbiserialr(binary, list1)
                            corrs.append(abs(corr))
                        correlations[allvarids[j]].append(max(corrs))
                    else:
                        exit(1)
        elif var1_type in ['single_choice', 'multiple_choice']:
            for j in range(len(allvarids)):
                var2_type = allvarlist[j]['subtype']
                if var2_type in ['decimal', 'numeric', 'scalar', 'labeled_scalar']:
                    corrs = []
                    thisopts = options_input_vars[i]
                    # print(thisopts)
                    list2 = [float(val) if val is not None else 0.0 for val in cleaned_dataset[j]]
                    for option in thisopts:
                        binary = []
                        for point in data_chosen[i]:
                            if point is None:
                                binary.append(0)
                            elif str(point).find(str(option[0])) != -1:
                                binary.append(1)
                            else:
                                binary.append(0)
                        corr, p = pointbiserialr(binary, list2)
                        corrs.append(abs(corr))
                    correlations[allvarids[j]].append(max(corrs))
        else:
            continue

    # print(correlations)

    tempdic = {}
    for key in correlations:
        if correlations[key] != []:
            if max(correlations[key]) >= threshold:
                tempdic[key] = max(correlations[key])
    correlations = tempdic

    if correlations == {}:
        print(json.dumps({"success": True, "message": None, "data": []}))
        return

    ordered = []
    for key, value in sorted(correlations.items(), key=lambda item: item[1]):
        ordered.append(key)
    cnx.close()

    highest = []
    for i in range(min(len(ordered), count)):
        highest.append(ordered[i])

    print(json.dumps({'success': True, 'message': None, 'data': highest}))
    return


if __name__ == "__main__":
    if test:
        ro(491)
        # ro(520)
        # suggest(1, 236, "5372", 0.55, 7)
    else:
        if len(sys.argv) == 1:
            exit(1)
        parser = argparse.ArgumentParser()
        parser.add_argument('--mode', '-m', help="", type=str, default="")
        parser.add_argument('--analysis', '-a', help="", type=int, default=0)
        parser.add_argument('--org', '-o', help="", type=int, default=0)
        parser.add_argument('--survey', '-s', help="", type=int, default=0)
        parser.add_argument('--variables', '-v', help="", type=str, default="")
        parser.add_argument('--threshold', '-t', help="", type=float, default=0.0)
        parser.add_argument('--no_vars', '-n', help="", type=int, default=0)
        args = parser.parse_args()
        mode = args.mode
        analysis_id = args.analysis
        org_id = args.org
        survey_id = args.survey
        variable_ids = args.variables
        threshold = args.threshold
        count = args.no_vars

        if mode == 'ro':
            ro(analysis_id)
        elif mode == 'suggest':
            suggest(org_id, survey_id, variable_ids, threshold, count)
        else:
            exit(1)