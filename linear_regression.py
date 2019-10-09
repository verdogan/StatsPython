from sperror import sperror
import mysql.connector
import itertools
import json
import numpy as np
import statsmodels.api as sm
import logging
from checkprint import checkprint
from checkwarns import checkwarns


checkwarns()


writelog = False

if not writelog:
    log = logging.getLogger()
    log.setLevel(logging.CRITICAL)


# function to label a number according to its sign, will be useful later
def sign(num):
    try:
        if num >= 0:
            return 1
        else:
            return 2
    except:
        return 'n/a'


# serializes the independent variables to make them suitable for the algorithm
def serialize(lst):
    newlst = []
    for element in lst:
        if isinstance(element[0], float):
            newlst.append(element)
        else:
            for sublst in element:
                newlst.append(sublst)
    return newlst


def linear_regression(conn, time, analysis_id, ivs, ivids, dv, data_raw_iv, data_raw_dv, options_iv):
    cnx = conn
    equations = []

    checkprint("Linear regression starts")

    counter = 0

    r_values = []

    # create and run the queries for each equation
    for L in range(2, len(data_raw_iv)+1):
        for subset in itertools.combinations(list(range(0, len(data_raw_iv))), L):
            X = list(data_raw_iv[i] for i in subset)
            X = serialize(X)
            minmax = []
            # find the highest and lowest values of an iv
            for col in X:
                mm = [min(col), max(col)]
                minmax.append(mm)
            # prepare X and y, X the matrix of ivs and y the vector of dv
            X = np.asarray(X)
            X = X.transpose()
            y = np.asarray(data_raw_dv[0]).transpose()
            X = sm.add_constant(X)
            if len(y) <= len(X[0]):
                cursor_err = cnx.cursor()
                cursor_err.execute(sperror("ERR_TOO_FEW_DATA", analysis_id))
                cnx.commit()
                cursor_err.close()
                cnx.commit()
                exit(1)
            # run the algorithm
            model = sm.GLS(y, X).fit()
            prediction = model.predict(X)
            # fetch the r_value of the model
            r_values.append(model.rsquared_adj)
            counter += 1
            includedivs = []
            # take note of ivs that are included in this iteration
            for i in range(0, len(ivids)):
                if i in list(subset):
                    includedivs.append(ivids[i])
            equations.append([model, prediction, includedivs, minmax])


    # find and list the ranks for each equation
    scores = []
    for [mdl, prediction, includedivs, minmax] in equations:
        scores.append(mdl.rsquared_adj)
    ranks = [score[0] for score in sorted(enumerate(scores[:-1]),key=lambda i:i[1])]
    ranks = list(reversed(ranks))
    ranks = [rank+2 for rank in ranks]
    ranks.append(1)

    i = 0
    for i in range(len(ranks)):
        equations[i].append(ranks[i])

    for i in range(len(ranks)):
        if len(equations[i][2]) == len(data_raw_iv):
            equations[i].append("")
        else:
            equations[i].append("")
        equations[i].append(0)

    # for each combination of ivs
    for [mdl, prediction, includedivs, minmax, rank, tmp, residerr] in equations:
        cursor = cnx.cursor()
        equation_vars = ""
        for i in includedivs:
            equation_vars = equation_vars + str(i) + ":"
        equation_vars = equation_vars[:-1]
        equation_values = ""
        # get coefficients of the equation
        for param in mdl.params:
            equation_values = equation_values + str(param) + ":"
        equation_values = equation_values[:-1]
        r_score = round(mdl.rsquared, 1)
        num_ivs = len(includedivs)
        adjusted_R2 = round(mdl.rsquared_adj, 2)
        num_rpe = residerr
        # find the partial contribution of each iv to th result for a given equation
        overall_contribution_dict = dict()
        summed = 0
        signs = []
        counter = 0
        # multiply the range of the iv with the absolute value of the coefficient
        for param in mdl.params[1:]:
            summed += abs(param)*(minmax[counter][1]-minmax[counter][0])
            counter += 1
        contributions = []
        counter = 0
        # calculate the contributions as percentage of total
        for param in mdl.params[1:]:
            contributions.append((abs(param)*(minmax[counter][1]-minmax[counter][0]) / summed) * 100)
            if param >= 0:
                signs.append(-1)
            else:
                signs.append(1)
            counter += 1
        # create the json string that contains coefficients and contributions
        inspection_data_dict = dict()
        counter = 1
        try:
            # for each iv in this equation
            for iv in includedivs:
                idx = ivids.index(iv)
                # if it's numerical
                if isinstance(data_raw_iv[idx][0], float):
                    inspection_data_dict[iv] = dict()
                    # is the contribution positive of negative
                    inspection_data_dict[iv]["b"] = sign(mdl.params[counter])
                    # coefficient
                    inspection_data_dict[iv]["c"] = mdl.params[counter]
                    # contribution
                    inspection_data_dict[iv]["t"] = int(contributions[counter-1])
                    # p-value
                    if np.isnan(mdl.pvalues[counter]):
                        inspection_data_dict[iv]["p"] = 1.00
                    else:
                        inspection_data_dict[iv]["p"] = mdl.pvalues[counter]
                    # range
                    inspection_data_dict[iv]["v"] = [str(int(minmax[counter-1][0])), str(int(minmax[counter-1][1]))]
                    overall_contribution_dict[iv] = int(contributions[counter - 1])
                    counter += 1
                # if it's categorical
                else:
                    inspection_data_dict[iv] = dict()
                    total_contribution = 0
                    options = []
                    # for each option of the iv
                    for option_value, option_label in options_iv[idx]:
                        inspection_data_dict[iv][option_value] = dict()
                        # is the contribution positive or negative
                        inspection_data_dict[iv][option_value]["b"] = sign(mdl.params[counter])
                        # coefficient
                        inspection_data_dict[iv][option_value]["c"] = mdl.params[counter]
                        # contribution
                        inspection_data_dict[iv][option_value]["t"] = int(contributions[counter-1])
                        # p-value
                        if np.isnan(mdl.pvalues[counter]):
                            inspection_data_dict[iv][option_value]["p"] = 1.00
                        else:
                            inspection_data_dict[iv][option_value]["p"] = mdl.pvalues[counter]
                        total_contribution += contributions[counter-1]
                        option_label_modified = option_label
                        options.append(option_label_modified)
                        counter += 1
                    # total contribution of the iv with all of its options combined
                    inspection_data_dict[iv]["t"] = int(total_contribution)
                    inspection_data_dict[iv]["v"] = options
                    overall_contribution_dict[iv] = int(total_contribution)
        except:
            checkprint("---")
            checkprint("Dictionaries can't be created")
            checkprint(includedivs)
            checkprint("---")
            continue
        inspection_data = json.dumps(inspection_data_dict)
        inspection_data = inspection_data.replace("'", r"\'")
        overall_contribution = json.dumps(overall_contribution_dict)

        # find the variables that contribute most to the dv
        important_vars = ""
        sorted_contribution = dict()
        # sort them according to the contribution in a descending order
        for w in sorted(overall_contribution_dict, key=overall_contribution_dict.get, reverse=True):
            sorted_contribution[w] = overall_contribution_dict[w]
        sorted_ids = list(sorted_contribution.keys())
        # get the variables with highest contribution
        for i in range(0, int(float(len(sorted_contribution)/3) + 1)):
            important_vars += str(sorted_ids[i])
            important_vars += ","
        important_vars = important_vars[:-1]

        # insert the model information to the database
        sort_order = rank
        query = """INSERT INTO regression_analysis_results SET analysis_id = {current_id}, option_value = 0,
                                    equation_vars = '{equation_vars}', equation_values  = '{equation_values}', 
                                    r_score = {r_score}, num_ivs = {num_ivs},
                                    adjusted_R2 = {adjusted_R2}, num_rpe = {num_rpe}, 
                                    overall_contribution = '{overall_contribution}', 
                                    important_vars = '{important_vars}', inspection_data = '{inspection_data}', 
                                    sort_order = {sort_order}""".format(
            current_id=analysis_id, equation_vars=equation_vars, equation_values=equation_values,
            r_score=r_score, num_ivs=num_ivs, adjusted_R2=adjusted_R2,
            num_rpe=num_rpe, overall_contribution=overall_contribution, important_vars=important_vars,
            inspection_data=inspection_data, sort_order=sort_order)
        try:
            cursor.execute(query)
            checkprint("Success")
        except mysql.connector.Error as e:
            checkprint("Results can't be stored")
            checkprint(e)
            continue
        cursor.close()

    cursor = cnx.cursor(dictionary=True)
    try:
        cursor.execute("""UPDATE analysis SET analysis_status = 'done',
                                error = NULL,
                                modified = '{current_time}' 
                                WHERE id = {current_id}""".format(current_time=time, current_id=analysis_id))
    except:
        exit(1)
    cnx.commit()
    cursor.close()