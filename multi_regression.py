import itertools
import json
import numpy as np
import statsmodels.api as sm
import logging
from checkprint import checkprint
from checkwarns import checkwarns
import mysql.connector


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


def multi_regression(conn, time, analysis_id, ivs, ivids, dv, data_raw_iv, data_raw_dv, options_iv, options_dv, mulchoopt):
    cnx = conn

    equations = []

    checkprint("Multiple regression starts")

    # create regression equations with all combinations of ivs
    for L in range(2, len(data_raw_iv)+1):
        for subset in itertools.combinations(list(range(0, len(data_raw_iv))), L):
            mapped = False
            X = list(data_raw_iv[i] for i in subset)
            X = serialize(X)
            minmax = []
            # find the highest and lowest values of an iv
            for col in X:
                mm = [min(col), max(col)]
                minmax.append(mm)
            # prepare X and y, X the matrix of ivs and y the vector of dv
            X = np.asarray(X).transpose()
            y = np.asarray(data_raw_dv[0]).transpose()
            # find the best-fitting equation for given ivs
            try:
                prototype = sm.MNLogit(y, X)
                model = prototype.fit(disp=False)
            except:
                checkprint("optimization has failed")
                continue
            prediction = True
            # get relevant information about the given equation
            try:
                summary = model.summary()
            except:
                checkprint("Model can't be summarized")
            try:
                parameters = model.params
            except:
                checkprint("Model can't be parametrized")
            try:
                bic = model.bic
            except:
                checkprint("BIC can't be calculated")
            # add the includid ivs in this iteration to a list
            includedivs = []
            for i in range(0, len(ivids)):
                if i in list(subset):
                    includedivs.append(ivids[i])
            equations.append([model, prediction, includedivs, minmax])


    # find the ranks
    scores = []
    for [mdl, prediction, includedivs, minmax] in equations:
        try:
            scores.append(mdl.bic)
        except:
            scores.append(0)
    # sort them to find the best ranking one
    ranks = [score[0] for score in sorted(enumerate(scores[:-1]), key=lambda i: i[1])]
    ranks = list(reversed(ranks))
    ranks = [rank + 2 for rank in ranks]
    ranks.append(1)


    # for residual errors
    i = 0
    if len(equations) > 0:
        for i in range(len(ranks)):
            equations[i].append(ranks[i])
        for i in range(len(ranks)):
            equations[i].append("none")
            residerror = 0
            equations[i].append(residerror)


    # create and run the queries for each equation
    for [mdl, prediction, includedivs, minmax, rank, color, residerr] in equations:
        try:
            cursor = cnx.cursor()
            # ivs
            equation_vars = ""
            for i in includedivs:
                equation_vars = equation_vars + str(i) + ":"
            equation_vars = equation_vars[:-1]
            # coefficients
            equation_values = ""
            # fetch the coefficients
            for param in mdl.params:
                equation_values = equation_values + str(param[0]) + ":"
            equation_values = equation_values[:-1]
            highlight = color
            # fetch r score for the equation
            r_score = round(mdl.prsquared, 1)
            if np.isnan(r_score):
                r_score = 0.0
            num_ivs = len(includedivs)
            adjusted_R2 = 0.00
            num_rpe = 0
            # find the partial contribution of each iv to th result for a given equation
            overall_contribution_dict = dict()
            signs = []
            summed = 0
            counter = 0
            # multiply the range of the iv with the absolute value of the coefficient
            for param in mdl.params:
                summed += abs(param)*(minmax[counter][1]-minmax[counter][0])
                counter += 1
            contributions = []
            counter = 0
            # calculate the contributions as percentage of total
            for param in mdl.params:
                contributions.append((abs(param) * (minmax[counter][1] - minmax[counter][0]) / summed) * 100)
                if param >= 0:
                    signs.append(-1)
                else:
                    signs.append(1)
                counter += 1
            inspection_data_dict = dict()
            counter = 0
            # create the json string that contains coefficients and contributions
            try:
                # for each iv in this equation
                for iv in includedivs:
                    idx = ivids.index(iv)
                    # if it's numerical
                    if isinstance(data_raw_iv[idx][0], float):
                        inspection_data_dict[iv] = dict()
                        # is the contribution positive of negative
                        inspection_data_dict[iv]["b"] = sign(mdl.params[counter][0])
                        # coefficient
                        inspection_data_dict[iv]["c"] = mdl.params[counter][0]
                        # contribution
                        inspection_data_dict[iv]["t"] = int(contributions[counter])
                        # p-value
                        if np.isnan(mdl.pvalues[counter][0]):
                            inspection_data_dict[iv]["p"] = 1.00
                        else:
                            inspection_data_dict[iv]["p"] = mdl.pvalues[counter][0]
                        # range
                        inspection_data_dict[iv]["v"] = [str(int(minmax[counter][0])), str(int(minmax[counter][1]))]
                        overall_contribution_dict[iv] = int(contributions[counter])
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
                            inspection_data_dict[iv][option_value]["b"] = sign(mdl.params[counter][0])
                            # coefficient
                            inspection_data_dict[iv][option_value]["c"] = mdl.params[counter][0]
                            # contribution
                            inspection_data_dict[iv][option_value]["t"] = int(contributions[counter])
                            # p-value
                            if np.isnan(mdl.pvalues[counter][0]):
                                inspection_data_dict[iv][option_value]["p"] = 1.00
                            else:
                                inspection_data_dict[iv][option_value]["p"] = mdl.pvalues[counter][0]
                            total_contribution += contributions[counter]
                            # option_label_modified = option_label.replace("'", "")
                            # option_label_modified = cnx.escape_string(option_label)
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
                checkprint(mulchoopt)
                checkprint("---")
                continue
            inspection_data = json.dumps(inspection_data_dict)
            inspection_data = inspection_data.replace("'", r"\'")
            overall_contribution = json.dumps(overall_contribution_dict)

            important_vars = ""
            sorted_contribution = dict()
            # determine the most important ivs in terms of contribution
            try:
                # sort them according to the contribution in a descending order
                for w in sorted(overall_contribution_dict, key=overall_contribution_dict.get, reverse=True):
                    sorted_contribution[w] = overall_contribution_dict[w]
                # get the variables with highest contribution
                for i in range(0, int(float(len(sorted_contribution)/3) + 1)):
                    important_vars = important_vars + str(list(sorted_contribution.keys())[i]) + ","
                important_vars = important_vars[:-1]
            except:
                pass

            sort_order = rank
            #run the query to save the results into the database
            query = """INSERT INTO regression_analysis_results SET analysis_id = {current_id}, option_value = {option_value}, 
                                            equation_vars = '{equation_vars}', equation_values  = '{equation_values}', 
                                            highlight = '{highlight}', r_score = {r_score}, num_ivs = {num_ivs},
                                            adjusted_R2 = {adjusted_R2}, num_rpe = {num_rpe}, 
                                            overall_contribution = '{overall_contribution}', 
                                            important_vars = '{important_vars}', inspection_data = '{inspection_data}', 
                                            sort_order = {sort_order}""".format(
                current_id=analysis_id, option_value = mulchoopt, equation_vars=equation_vars, equation_values=equation_values,
                highlight=highlight, r_score=r_score, num_ivs=num_ivs, adjusted_R2=adjusted_R2,
                num_rpe=num_rpe, overall_contribution=overall_contribution, important_vars=important_vars,
                inspection_data=inspection_data, sort_order=sort_order)
            try:
                cursor.execute(query)
                checkprint("---")
                checkprint("query successful")
                checkprint(query)
                checkprint(includedivs)
                checkprint(mulchoopt)
                checkprint("---")
            except mysql.connector.Error as e:
                checkprint("---")
                checkprint("query problem")
                checkprint(query)
                print(e)
                checkprint(includedivs)
                checkprint(mulchoopt)
                checkprint("---")
                cursor.close()
            cursor.close()
        except:
            checkprint("---")
            checkprint("general problem")
            checkprint(includedivs)
            checkprint(mulchoopt)
            checkprint("---")
            continue

    cursor = cnx.cursor(dictionary=True)
    try:
        cursor.execute("""UPDATE analysis SET analysis_status = 'done',
                                    error = NULL,
                                    modified = '{current_time}' 
                                    WHERE id = {current_id}""".format(current_time=time, current_id=analysis_id))
    except:
        checkprint("Database can't be updated")
        exit(1)
    cnx.commit()
    cursor.close()