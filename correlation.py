from scipy.stats.stats import pearsonr
from connection import connect
import time
from datetime import datetime
from pytz import timezone


def twocorr(x, y):
    try:
        corr, p = pearsonr(x, y)
    except:
        exit(1)

    return corr, p


tz = timezone('EST')
now_est = datetime.now(tz)
now = now_est.strftime("%Y-%m-%d %H:%M:%S")

analysis_id = 1

cnx = connect()

cursor1 = cnx.cursor(dictionary=True)
cursor1.execute("SELECT * FROM analysis WHERE id = {current_id}".format(current_id = analysis_id))
result = cursor1.fetchall()
cursor1.close()

if result[0]["analysis_type"] != "CORRELATION":
    cursor2 = cnx.cursor()
    cursor2.execute("""UPDATE analysis SET analysis_status = 'error',
                        error = 'Analysis is not Correlational',
                        modified = '{current_time}' 
                        WHERE id = {current_id}""".format(current_time=now, current_id=analysis_id))
    cnx.commit()
    cursor2.close()
    cnx.close()
    exit(1)

cursor3 = cnx.cursor(dictionary=True)
cursor3.execute("""UPDATE analysis SET analysis_status = 'running',
                        error = NULL,
                        modified = '{current_time}' 
                        WHERE id = {current_id}""".format(current_time=now, current_id=analysis_id))
cnx.commit()
cursor3.close()

cursor4 = cnx.cursor()
cursor4.execute("""DELETE FROM correlation_analysis_results 
                    WHERE analysis_id = {current_id}""".format(current_id=analysis_id))
cnx.commit()
cursor4.close()

cursor5 = cnx.cursor(dictionary=True, buffered=True)
ro_id = result[0]["ro_id"]
cursor5.execute("SELECT variable_id FROM ro_vars WHERE ro_id = {current_ro_id}".format(current_ro_id=ro_id))
variables = cursor5.fetchall()
cursor5.close()

# TODO implement workbook (level 1) filters

# [{'filter_criteria_id': 1, 'applies_to': 'RO', 'ro_id': 1, 'workbook_id': None, 'filter_var': 156,
# 'filter_condition': 'isgreaterthan', 'value1': '1000', 'value2': None,
# 'created': datetime.datetime(2018, 8, 7, 15, 1, 37), 'modified': None}]
# This result means that _values that are > 1000 should be ***removed/excluded*** from the values of variable
# whose id is 156
cursor7 = cnx.cursor(dictionary=True, buffered=True)
cursor7.execute("SELECT * FROM ro_filter WHERE ro_id = {current_ro_id}".format(current_ro_id=ro_id))
filters2 = cursor7.fetchall()
print(filters2[1])
cursor7.close()

data_raw = []
for variable in variables:
    cursor8 = cnx.cursor(dictionary=True, buffered=True)
    cursor8.callproc('GetVariableData', [variable["variable_id"], result[0]["org_id"], result[0]["survey_id"]])
    for i in cursor8.stored_results():
        data_raw.append(i.fetchall())
    cursor8.close()
for var1 in data_raw:
    for var2 in data_raw:
        if len(var1) != len(var2):
            exit(1)
data_with_nones = []
for variable in data_raw:
    row = []
    for value in variable:
        if value[0] is not None:
            row.append(int(value[0]))
        else:
            row.append(None)
    data_with_nones.append(row)


data = data_with_nones
for i in range(0, len(variables)):
    for j in range(0, len(variables)):
        cursor9 = cnx.cursor()
        tempvar1 = []
        tempvar2 = []
        for k in range(0, len(data[i])):
            if data[i][k] != None and data[j][k] != None:
                tempvar1.append(data[i][k])
                tempvar2.append(data[j][k])
        corr, p = twocorr(tempvar1, tempvar2)
        corr = round(corr, 2)
        p = round(p, 2)
        cursor9.execute("""INSERT INTO correlation_analysis_results SET analysis_id = {current_id}, 
                            var1 = {var1}, var2 = {var2}, corr_value = {corr_value}, 
                            p_value = {p_value}""".format(current_id=analysis_id,
                            var1=variables[i]["variable_id"], var2=variables[j]["variable_id"],
                            corr_value=corr, p_value=p))
        cnx.commit()
        cursor9.close()

cursor10 = cnx.cursor(dictionary=True)
cursor10.execute("""UPDATE analysis SET analysis_status = 'done',
                        error = NULL,
                        modified = '{current_time}' 
                        WHERE id = {current_id}""".format(current_time=now, current_id=analysis_id))
cnx.commit()
cursor10.close()

cnx.close()

exit(0)
