from scipy.stats import pearsonr
from connection import connect, currtime
from sperror import sperror
import sys

cnx = connect()

# analysis_id = sys.argv[1]
analysis_id = 2


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
    cursor5.execute("SELECT variable_id FROM ro_vars WHERE ro_id = {current_ro_id}".format(current_ro_id=ro_id))
except:
    cursor_err = cnx.cursor()
    cursor_err.execute(sperror("A variable can't be retrieved", analysis_id))
    cnx.commit()
    cursor_err.close()
    cnx.close()
    exit(1)
variables = cursor5.fetchall()
cursor5.close()

# level 2 filters commented out until deciding how to implement
# [{'filter_criteria_id': 1, 'applies_to': 'RO', 'ro_id': 1, 'workbook_id': None, 'filter_var': 156,
# 'filter_condition': 'isgreaterthan', 'value1': '1000', 'value2': None,
# 'created': datetime.datetime(2018, 8, 7, 15, 1, 37), 'modified': None}]
# This result means that _values that are > 1000 should be ***removed/excluded*** from the values of variable
# whose id is 156
#cursor7 = cnx.cursor(dictionary=True, buffered=True)
#cursor7.execute("SELECT * FROM ro_filter WHERE ro_id = {current_ro_id}".format(current_ro_id=ro_id))
#filters2 = cursor7.fetchall()
#print(filters2[1])
#cursor7.close()

data_raw = []
for variable in variables:
    cursor8 = cnx.cursor(dictionary=True, buffered=True)
    try:
        cursor8.callproc('GetVariableData', [variable["variable_id"], result[0]["org_id"], result[0]["survey_id"]])
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("Variables can't be retrieved", analysis_id))
        cnx.commit()
        cursor_err.close()
        cnx.close()
        exit(1)
    for i in cursor8.stored_results():
        data_raw.append(i.fetchall())
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
            row.append(float(value[0]))
        else:
            row.append(None)
    data_with_nones.append(row)


data = data_with_nones
for i in range(0, len(variables)):
    for j in range(i, len(variables)):
        cursor91 = cnx.cursor()
        cursor92 = cnx.cursor()
        if i == j:
            corr = 1.00
            p = 0.00
        else:
            tempvar1 = []
            tempvar2 = []
            for k in range(0, len(data[i])):
                if data[i][k] != None and data[j][k] != None:
                    tempvar1.append(data[i][k] + 0.000001)
                    tempvar2.append(data[j][k] + 0.000001)
            corr, p = twocorr(tempvar1, tempvar2)
            corr = round(corr, 2)
            p = round(p, 2)
        print(corr)
        try:
            cursor91.execute("""INSERT INTO correlation_analysis_results SET analysis_id = {current_id}, 
                                var1 = {var1}, var2 = {var2}, corr_value = {corr_value}, 
                                p_value = {p_value}""".format(current_id=analysis_id,
                                var1=variables[i]["variable_id"], var2=variables[j]["variable_id"],
                                corr_value=corr, p_value=p))
            if i != j:
                cursor92.execute("""INSERT INTO correlation_analysis_results SET analysis_id = {current_id}, 
                                    var1 = {var1}, var2 = {var2}, corr_value = {corr_value}, 
                                    p_value = {p_value}""".format(current_id=analysis_id,
                                    var1=variables[j]["variable_id"], var2=variables[i]["variable_id"],
                                    corr_value=corr, p_value=p))
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
    print("Can't finalize the analysis")
    exit(1)
cnx.commit()
cursor10.close()

cnx.close()

exit(0)
