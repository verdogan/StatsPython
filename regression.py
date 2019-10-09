from connection import connect, currtime
from sperror import sperror
from linear_regression import linear_regression
from multi_regression import multi_regression
import sys
import random
import logging
from checkprint import checkprint
from checkwarns import checkwarns


checkwarns()


writelog = False

if not writelog:
    log = logging.getLogger()
    log.setLevel(logging.CRITICAL)


test = False


if test is True:
    ans_id = 474
else:
    # terminate if no arguments are given
    if len(sys.argv) != 2:
        exit(1)
    ans_id = sys.argv[1]

try:
    analysis_id = int(ans_id)
except:
    exit(1)

if analysis_id < 1:
    exit(1)


def regression(regression_id):
    cnx = connect()
    now = currtime()

    # try starting analysis
    cursor = cnx.cursor(dictionary=True)
    try:
        cursor.execute("""UPDATE analysis SET analysis_status = 'running',
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
    cursor.close()

    # remove old results
    cursor = cnx.cursor()
    try:
        cursor.execute("""DELETE FROM regression_analysis_results 
                            WHERE analysis_id = {current_id}""".format(current_id=analysis_id))
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("Can't access or modify analysis results", analysis_id))
        cnx.commit()
        cursor_err.close()
        cnx.close()
        exit(1)
    cnx.commit()
    cursor.close()

    # select given analysis id
    cursor = cnx.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM analysis WHERE id = {current_id} AND analysis_type = 'REGRESSION'"
                        .format(current_id = analysis_id))
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

    # fetch dependent variable
    cursor = cnx.cursor(dictionary=True, buffered=True)
    try:
        cursor.execute("""SELECT var_id AS id, var_type AS type, var_subtype AS subtype FROM ro_vars
                           INNER JOIN var_list ON var_list.var_id = ro_vars.variable_id
                           WHERE ro_vars.ro_id = {current_ro_id} AND ro_vars.analysis_var_type = 'DV'"""
                           .format(current_ro_id=ro_id))
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("DV can't be retrieved", analysis_id))
        cnx.commit()
        cursor_err.close()
        cnx.close()
        exit(1)
    dv = cursor.fetchall()
    cursor.close()
    checkprint(dv)

    # fetch independent variables
    ivids = []
    cursor = cnx.cursor(dictionary=True, buffered=True)
    ro_id = result[0]["ro_id"]
    try:
        cursor.execute("""SELECT var_id AS id, var_type AS type, var_subtype AS subtype FROM ro_vars
                           INNER JOIN var_list ON var_list.var_id = ro_vars.variable_id
                           WHERE ro_vars.ro_id = {current_ro_id} AND ro_vars.analysis_var_type = 'IV'"""
                           .format(current_ro_id=ro_id))
    except:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("IV can't be retrieved", analysis_id))
        cnx.commit()
        cursor_err.close()
        cnx.close()
        exit(1)
    ivs = cursor.fetchall()
    cursor.close()
    for iv in ivs:
        ivids.append(iv['id'])

    # get options for categorical independent variables
    options_iv = []
    for variable in ivs:
        cursor = cnx.cursor(dictionary=True, buffered=True)
        if True:
            try:
                cursor.callproc('GetVariableOptions', [variable["id"]])
            except:
                cursor_err = cnx.cursor()
                cursor_err.execute(sperror("Variable options can't be retrieved", analysis_id))
                cnx.commit()
                cursor_err.close()
                cnx.close()
                exit(1)
            for i in cursor.stored_results():
                options_iv.append(i.fetchall())
        cursor.close()
    checkprint(options_iv)

    # get options for categorical dependent variables
    options_dv = []
    cursor = cnx.cursor(dictionary=True, buffered=True)
    if True:
        try:
            cursor.callproc('GetVariableOptions', [dv[0]["id"]])
        except:
            cursor_err = cnx.cursor()
            cursor_err.execute(sperror("Variable options can't be retrieved", analysis_id))
            cnx.commit()
            cursor_err.close()
            cnx.close()
            exit(1)
        for i in cursor.stored_results():
            options_dv.append(i.fetchall())
    cursor.close()

    # fetch data points for independent variables
    data_raw_iv = []
    for i in range(0, len(ivs)):
        variable = ivs[i]
        cursor = cnx.cursor(dictionary=True, buffered=True)
        try:
            # cursor.callproc('GetVariableData', [variable["id"], org_id, survey_id])
            cursor.callproc('GetVariableDataF', [variable["id"], org_id, survey_id, ro_id, 0, ''])
        except:
            cursor_err = cnx.cursor()
            cursor_err.execute(sperror("IV content can't be retrieved", analysis_id))
            cnx.commit()
            cursor_err.close()
            cnx.close()
            exit(1)
        for j in cursor.stored_results():
            fetched = j.fetchall()
            # for numerical variables
            if ivs[i]['subtype'] in ['decimal', 'numeric', 'scalar', 'labeled_scalar']:
                fetched = [float(x[0]) if x[0] is not None else float(random.gauss(0, 1)) for x in fetched]
            # for categorical variables
            elif ivs[i]['subtype'] in ['single_choice', 'multiple_choice', 'text_scalar']:
                binaries = []
                for option_id, option_label in options_iv[i]:
                    binary = []
                    for point in fetched:
                        try:
                            # binarize each option for each categorical variable
                            if point[0] == option_id:
                                binary.append(1.0)
                            else:
                                binary.append(0.0)
                        except:
                            binary.append(0.0)
                    binaries.append(binary)
                fetched = binaries
            else:
                exit(1)
            data_raw_iv.append(fetched)
        cursor.close()

    # get type of the dependent variable
    dv_type = dv[0]['subtype']
    mulchoopts = []

    # fetch data points for dependent variable
    data_raw_dv = []
    for variable in dv:
        cursor = cnx.cursor(dictionary=True, buffered=True)
        try:
            # cursor.callproc('GetVariableData', [variable["id"], org_id, survey_id])
            cursor.callproc('GetVariableDataF', [variable["id"], org_id, survey_id, ro_id, 0, ''])
        except:
            cursor_err = cnx.cursor()
            cursor_err.execute(sperror("DV content can't be retrieved", analysis_id))
            cnx.commit()
            cursor_err.close()
            cnx.close()
            exit(1)
        for i in cursor.stored_results():
            fetched = i.fetchall()
        if dv_type in ['decimal', 'numeric', 'scalar', 'labeled_scalar']:
            fetched = [float(x[0]) if x[0] is not None else float(random.gauss(0, 1)) for x in fetched]
            data_raw_dv.append(fetched)
        elif dv_type in ['single_choice', 'text_scalar', 'multiple_choice']:
            for option_id, option_label in options_dv[0]:
                mulchoopts.append(option_id)
            checkprint(mulchoopts)
            for mulchoopt in mulchoopts:
                binary = []
                for point in fetched:
                    try:
                        # binarize options for categorical variables
                        if point[0] == mulchoopt:
                            binary.append(1.0)
                        else:
                            binary.append(0.0)
                    except:
                        binary.append(0.0)
                data_raw_dv.append([binary])
        else:
            exit(1)
        cursor.close()


    # start the appropriate analysis according to the type of the dependent variable
    if dv_type in ['decimal', 'numeric', 'scalar', 'labeled_scalar']:
        if len(ivids) >= len(data_raw_dv[0]):
            try:
                cursor_err = cnx.cursor()
                cursor_err.execute(sperror("ERR_TOO_FEW_DATA", analysis_id))
                cnx.commit()
                cursor_err.close()
                checkprint("error can be saved")
            except:
                checkprint("error can't be saved")
            cnx.close()
            exit(1)
        try:
            # start linear regression for numerical dv
            linear_regression(cnx, now, regression_id, ivs, ivids, dv,
                              data_raw_iv, data_raw_dv, options_iv)
        except:
            exit(1)
    elif dv_type in ['single_choice', 'text_scalar', 'multiple_choice']:
        if len(ivids) >= len(data_raw_dv[0][0]):
            try:
                cursor_err = cnx.cursor()
                cursor_err.execute(sperror("ERR_TOO_FEW_DATA", analysis_id))
                cnx.commit()
                cursor_err.close()
                checkprint("error can be saved")
            except:
                checkprint("error can't be saved")
            cnx.close()
            exit(1)
        for i in range(0, len(mulchoopts)):
            # start multiple regression for each option of categorical dv
            multi_regression(cnx, now, regression_id, ivs, ivids, dv,
                             data_raw_iv, data_raw_dv[i], options_iv, options_dv, mulchoopts[i])
    else:
        cursor_err = cnx.cursor()
        cursor_err.execute(sperror("Unknown DV type", analysis_id))
        cnx.commit()
        cursor_err.close()
        cnx.commit()
        exit(1)

    try:
        cnx.close()
    except:
        exit(1)


regression(analysis_id)