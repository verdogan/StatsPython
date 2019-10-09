import sys
import argparse
from scipy.stats import chi2_contingency
from connection import connect, currtime
from sperror import sperror
from itertools import combinations

test = False


def ro(analysis_id, variable_ids):
    cnx, now = connect(), currtime()
    result, ro_id, survey_id, org_id = select_from_analysis(analysis_id)
    update_analysis_running(analysis_id)
    delete_results(analysis_id)
    variables = get_varinfo_from_varids(variable_ids, analysis_id)
    variable_ids = variable_ids.split(',')
    options = get_options(variable_ids)
    data = get_variable_dataf(variable_ids, ro_id, survey_id, org_id, analysis_id)
    check_empty_var(data, variables, cnx)
    orders = list(range(len(variable_ids)))

    for subset in combinations(orders, 2):
        (first, second) = subset
        print(subset)
        subids = [variable_ids[i] for i in [first, second]]
        subdata = [data[i] for i in [first, second]]
        subopts = [options[i] for i in [first, second]]
        counts = find_counts(subdata, subopts)
        percentages = find_percentages(counts)
        try:
            chi2, p, dof, ex = chi2_contingency(counts, correction=False)
        except:
            cursor_err = cnx.cursor()
            message = "Algorithm has failed"
            cursor_err.execute(sperror(message, analysis_id))
            cnx.commit()
            cursor_err.close()
            exit(1)
        p = round(p, 4)
        save_results_as_corr(p, percentages, survey_id, subids, subopts, analysis_id)

    update_analysis_done(analysis_id)
    return


def check_empty_var(data, variables, cnx):
    for i in range(len(data)):
        if data[i].count(None) == len(data[i]):
            cursor_err = cnx.cursor()
            message = "Variable " + variables[i]['name'] + " has no data"
            cursor_err.execute(sperror(message, analysis_id))
            cnx.commit()
            cursor_err.close()
            exit(1)
    return


def select_from_analysis(analysis_id):
    cnx, now = connect(), currtime()
    cursor = cnx.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM analysis WHERE id = {current_id}"
                       .format(current_id=analysis_id))
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("Could not find analysis in database", analysis_id))
        cnx.commit()
        cursor_err.close()
        exit(1)
    result = cursor.fetchall()
    ro_id = result[0]["ro_id"]
    survey_id = result[0]["survey_id"]
    org_id = result[0]["org_id"]
    cursor.close()
    cnx.close()
    return result, ro_id, survey_id, org_id


def update_analysis_running(analysis_id):
    cnx, now = connect(), currtime()
    cursor = cnx.cursor(dictionary=True)
    try:
        cursor.execute("""UPDATE analysis SET analysis_status = 'running', error = NULL,
                       modified = '{current_timestamp}' WHERE id = {current_id}"""
                       .format(current_timestamp=now, current_id=analysis_id))
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("Analysis couldn't be updated", analysis_id))
        cnx.commit()
        cursor_err.close()
        exit(1)
    cnx.close()
    return


def update_analysis_done(analysis_id):
    cnx, now = connect(), currtime()
    cursor = cnx.cursor(dictionary=True)
    try:
        cursor.execute("""UPDATE analysis SET analysis_status = 'done', error = NULL,
                       modified = '{current_timestamp}' WHERE id = {current_id}"""
                       .format(current_timestamp=now, current_id=analysis_id))
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("Analysis couldn't be updated", analysis_id))
        cnx.commit()
        cursor_err.close()
        exit(1)
    cnx.close()
    return


def delete_results(analysis_id):
    cnx, now = connect(), currtime()
    cursor = cnx.cursor(dictionary=True)
    try:
        cursor.execute("""DELETE FROM correlation_analysis_results 
                       WHERE analysis_id = {current_id} AND is_chisquare = 1"""
                       .format(current_id=analysis_id))
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("Old results couldn't be deleted", analysis_id))
        cnx.commit()
        cursor_err.close()
        exit(1)
    cnx.close()
    return


def get_varinfo_from_varids(variable_ids, analysis_id):
    cnx, now = connect(), currtime()
    cursor = cnx.cursor(dictionary=True, buffered=True)
    try:
        cursor.execute("""SELECT var_id AS id, var_type AS type, var_subtype AS subtype,
                        var_name AS name FROM var_list WHERE var_id IN ({current_var_ids})"""
                       .format(current_var_ids=variable_ids))
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("A variable can't be retrieved", analysis_id))
        cnx.commit()
        cursor_err.close()
        cnx.close()
        exit(1)
    variables = cursor.fetchall()
    cursor.close()
    cnx.close()
    return variables


def get_options(variable_ids):
    cnx, now = connect(), currtime()
    options = []
    for variable_id in variable_ids:
        cursor = cnx.cursor(dictionary=True, buffered=True)
        if True:
            try:
                cursor.callproc('GetVariableOptions', [variable_id])
            except:
                cursor_err = cnx.cursor()
                cnx.commit()
                cursor_err.close()
                cnx.close()
                print("---")
                exit(1)
            for i in cursor.stored_results():
                options.append(i.fetchall())
        cursor.close()
    cnx.close()
    return options


def get_variable_dataf(variable_ids, ro_id, survey_id, org_id, analysis_id):
    cnx, now = connect(), currtime()
    data = []
    for variable_id in variable_ids:
        cursor = cnx.cursor(dictionary=True, buffered=True)
        try:
            cursor.callproc('GetVariableDataF', [variable_id, org_id, survey_id, ro_id, 0, ''])
        except:
            cursor_err = cnx.cursor()
            cursor_err.execute(sperror("Variable data can't be retrieved", analysis_id))
            cnx.commit()
            cursor_err.close()
            cnx.close()
            exit(1)
        for i in cursor.stored_results():
            fetched = i.fetchall()
            fetched = [('1',) if element[0] is None else element for element in fetched]
            data.append(fetched)
        cursor.close()
    cnx.close()

    cleaned = []
    for variable in data:
        ints = []
        for data_point in variable:
            if data_point is not None:
                ints.append(int(data_point[0]))
            else:
                ints.append(1)
        cleaned.append(ints)

    return cleaned


def save_results_as_corr(p, percentages, survey_id, variable_ids, options, analysis_id):
    cnx, now = connect(), currtime()
    for i in range(len(options[0])):
        for j in range(len(options[1])):
            cursor1 = cnx.cursor()
            cursor2 = cnx.cursor()
            query1 = """INSERT INTO correlation_analysis_results SET analysis_id = {current_id},
                       var1 = {var1}, var2 = {var2},  var1_option = {var1_option}, var2_option = {var2_option},
                       p_value = {p_value}, percentage = {percentage}, is_chisquare = 1""".format(
                       current_id=analysis_id, var1=variable_ids[0], var2=variable_ids[1],
                       var1_option=i+1, var2_option=j+1, p_value=p, percentage=round(percentages[i][j], 2))
            query2 = """INSERT INTO correlation_analysis_results SET analysis_id = {current_id},
                                   var1 = {var2}, var2 = {var1},  var1_option = {var2_option}, var2_option = {var1_option},
                                   p_value = {p_value}, percentage = {percentage}, is_chisquare = 1""".format(
                current_id=analysis_id, var1=variable_ids[0], var2=variable_ids[1],
                var1_option=i + 1, var2_option=j + 1, p_value=p, percentage=round(percentages[i][j], 2))
            try:
                cursor1.execute(query1)
                cursor2.execute(query2)
                cnx.commit()
            except:
                exit(1)
            cursor1.close()
            cursor2.close()
    cnx.close()
    return


#####


def get_params(analysis_id):
    result, ro_id, survey_id, org_id = select_from_analysis(analysis_id)
    cnx, now = connect(), currtime()
    cursor = cnx.cursor(dictionary=True, buffered=True)
    ro_id = result[0]["ro_id"]
    try:
        cursor.execute("""SELECT var_id AS id, var_type AS type, var_subtype AS subtype FROM ro_vars 
                        INNER JOIN var_list ON var_list.var_id = ro_vars.variable_id 
                        WHERE ro_vars.ro_id = {current_ro_id}""".format(current_ro_id=ro_id))
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("A variable can't be retrieved", analysis_id))
        cnx.commit()
        cursor_err.close()
        cnx.close()
        exit(1)
    variables = cursor.fetchall()
    cursor.close()

    variable_ids = str(variables[0]["id"]) + "," + str(variables[1]["id"])

    cnx.close()
    return org_id, survey_id, variable_ids


def find_percentages(counts):
    sum = 0
    percentages = []
    for row in counts:
        for elem in row:
            sum += elem
    for i in range(len(counts)):
        percent_row = []
        for j in range(len(counts[i])):
            percent_row.append(100*counts[i][j]/sum)
        percentages.append(percent_row)
    return percentages


def find_counts(dataset, options):
    counts = []
    for i in range(len(options[0])):
        row = []
        for j in range(len(options[1])):
            row.append(0)
        counts.append(row)
    for i in range(len(dataset[0])):
        first = dataset[0][i]
        second = dataset[1][i]
        counts[first-1][second-1] += 1
    return counts


switch = 2

if switch == 0:
    if test:
        analysis_id = 325
        org_id, survey_id, variable_ids = get_params(analysis_id)
        main(org_id, survey_id, variable_ids, analysis_id)
    else:
        if len(sys.argv) != 2:
            exit(1)
        analysis_id = sys.argv[1]
        org_id, survey_id, variable_ids = get_params(325)
        main(org_id, survey_id, variable_ids, analysis_id)
elif switch == 1:
    if test:
        org_id = 1
        survey_id = 11
        variable_ids = ""
        for i in range(611, 613):
            variable_ids += str(i)
            variable_ids += ","
        variable_ids = variable_ids[:-1]
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument('--org', '-o', help="", type=int, default=0)
        parser.add_argument('--survey', '-s', help="", type=int, default=0)
        parser.add_argument('--variables', '-v', help="", type=str, default="")
        args = parser.parse_args()
        org_id = args.org
        survey_id = args.survey
        variable_ids = args.variables
    print(main(org_id, survey_id, variable_ids, 0))
elif switch == 2:
    if test:
        mode = "ro"
        ro(200, "931,932")
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument('--mode', '-m', help="", type=str, default=0)
        parser.add_argument('--analysis', '-a', help="", type=int, default=0)
        parser.add_argument('--variables', '-v', help="", type=str, default="")
        args = parser.parse_args()
        mode = args.mode
        analysis_id = args.analysis
        variable_ids = args.variables
        if mode == "ro":
            ro(analysis_id, variable_ids)
else:
    exit(1)