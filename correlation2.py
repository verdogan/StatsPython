from scipy.stats import pearsonr, pointbiserialr
from connection import connect, currtime
from sperror import sperror
import sys
from math import isnan

#if len(sys.argv) == 1:
    #print("No id was passed.")
    #exit(1)

#analysis_id = sys.argv[1]

analysis_id = 23

try:
    analysis_id = int(analysis_id)
except:
    print(("Invalid Analysis id ({this_id}) was passed").format(this_id=analysis_id))
    exit(1)

if analysis_id < 1:
    print(("Invalid Analysis id ({this_id}) was passed").format(this_id=analysis_id))
    exit(1)

def twocorr(x, y):
    try:
        corr, p = pearsonr(x, y)
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("Correlation can't be done", analysis_id))
        cnx.commit()
        cursor_err.close()
        cnx.close()
        exit(1)

    return corr, p

cnx = connect()
now = currtime()

cursor1 = cnx.cursor(dictionary=True)
try:
    cursor1.execute("SELECT * FROM analysis WHERE id = {current_id}".format(current_id = analysis_id))
except:
    print("Analysis can't be found")
    exit(1)
result = cursor1.fetchall()
cursor1.close()

if result[0]["analysis_type"] != "CORRELATION":
    cursor2 = cnx.cursor()
    cursor2.execute(sperror("Analysis is not Correlational", analysis_id))
    cnx.commit()
    cursor2.close()
    cnx.close()
    exit(1)

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

options = []
for variable in variables:
    print(variable)
    cursor8 = cnx.cursor(dictionary=True, buffered=True)
    if True: #check if the variable is categorical
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
print(options)


data_raw = []
j = 0
for variable in variables:
    cursor8 = cnx.cursor(dictionary=True, buffered=True)
    try:
        cursor8.callproc('GetVariableData', [variable["id"], result[0]["org_id"], result[0]["survey_id"]])
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("Variables can't be retrieved", analysis_id))
        cnx.commit()
        cursor_err.close()
        cnx.close()
        exit(1)
    for i in cursor8.stored_results():
        data_raw.append(i.fetchall())
        print(j)
        print(data_raw[j])
        print(len(data_raw[j]))
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
            # row.append(float(value[0]))
            row.append(value[0])
        else:
            row.append(None)
    data_with_nones.append(row)
data = data_with_nones

print(data)

added = []
for i in range(0, len(variables)):
    for j in range(0, len(variables)):
        if variables[j]['subtype'] == 'decimal' and (j, i) not in added:  # is j numerical? if not, continue
            tempvar_i = []
            tempvar_j = []
            results = []
            for k in range(0, len(data[i])):
                if data[i][k] is not None and data[j][k] is not None:
                    tempvar_i.append(data[i][k])
                    tempvar_j.append(data[j][k])
                else:  # deneme amacli
                    tempvar_i.append(data[i][k])
                    tempvar_j.append(data[j][k])
            # check to see if any of two arrays have all their values equal, like [1,1,1,1,1]
            if tempvar_i[1:] == tempvar_i[:-1] or tempvar_j[1:] == tempvar_j[:-1]:
                corr, p = 0.0, 0.0
                results.append([corr, p])
            # check to see if two arrays are identical, like [1,2,3,4,5], [1,2,3,4,5]
            elif tempvar_i == tempvar_j:
                corr, p = 1.0, 0.0
                results.append([corr, p])
            else:
                tempvar_j = [float(v) for v in tempvar_j]
                if variables[i]['subtype'] == 'decimal':  # numerical-numerical
                    tempvar_i = [float(v) for v in tempvar_i]
                    tempvar_j = [float(v) for v in tempvar_j]
                    corr, p = twocorr(tempvar_i, tempvar_j)
                    corr = round(corr, 2)
                    p = round(p, 2)
                    results.append([corr, p])
                elif variables[i]['subtype'] in ['single_choice', 'multiple_choice']:  # categorical-numerical
                    for opt in range(1, len(options[i]) + 1):
                        binary = []
                        for point in data[i]:
                            if point is None:
                                binary.append(0)
                            elif point.find(str(opt)) != -1:
                                binary.append(1)
                            else:
                                binary.append(0)
                        # results'a biserial testin sonuclarini ekle
                        print(binary)
                        print(tempvar_j)
                        corr, p = pointbiserialr(binary, tempvar_j)
                        corr = round(corr, 2)
                        p = round(p, 2)
                        results.append([corr, p])
                else:
                    exit(1)
            if variables[i]['subtype'] == 'decimal':
                option_end = 2
            else:
                option_end = len(options[i]) + 1
            for k in range(1, option_end):  # number of options of ith variable, 1 for numerical
                try:
                    if variables[i]['subtype'] == 'decimal':
                        i_option = 0
                        result_count = 0
                    else:
                        i_option = k
                        result_count = k - 1
                    cursor91 = cnx.cursor()
                    cursor92 = cnx.cursor()
                    print((i, j))
                    cursor91.execute("""INSERT INTO correlation_analysis_results SET analysis_id = {current_id}, 
                                        var1 = {var1}, var2 = {var2}, var1_option = {var1_option}, 
                                        corr_value = {corr_value}, p_value = {p_value}""".format(
                                        current_id=analysis_id, var1=variables[i]["id"], var2=variables[j]["id"],
                                        var1_option=i_option,
                                        corr_value=results[result_count][0],
                                        p_value=results[result_count][1]))
                    if i != j:
                        cursor92.execute("""INSERT INTO correlation_analysis_results SET analysis_id = {current_id}, 
                                            var1 = {var1}, var2 = {var2}, var1_option = {var1_option}, 
                                            corr_value = {corr_value}, p_value = {p_value}""".format(
                                            current_id=analysis_id, var1=variables[j]["id"], var2=variables[i]["id"],
                                            var1_option=i_option,
                                            corr_value=results[result_count][0],
                                            p_value=results[result_count][1]))
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
cnx.close()
exit(0)