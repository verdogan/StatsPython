import json
import argparse
import numpy as np
from scipy.stats import beta
from connection import connect, currtime
from sperror import sperror
from bucketize import bucketize
import re
import mysql.connector


test = False


cnx = connect()
now = currtime()


def main(org_id, survey_id, ro_id, segment_ids, analysis_id):
    if analysis_id is None:
        is_ro = False
    else:
        is_ro = True
        analysis_id = int(analysis_id)

    if is_ro:
        cursor = cnx.cursor(dictionary=True)
        try:
            cursor.execute("SELECT * FROM analysis WHERE id = {current_id}".format(current_id=analysis_id))
        except:
            exit(1)
        result = cursor.fetchall()
        cursor.close()
        # terminate if the analysis type is not segment
        if result[0]["analysis_type"] != "SEGMENT":
            cursor = cnx.cursor()
            cursor.execute(sperror("Analysis is not segment", analysis_id))
            cnx.commit()
            cursor.close()
            cnx.close()
            exit(1)
        # start the analysis
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
            cursor.execute("""DELETE FROM segment_diff_results 
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

    dataset1, dataset2, varlist, options, filters = getdata(org_id, survey_id, ro_id, segment_ids, analysis_id)
    buckets = create_buckets(dataset1, varlist)
    scores = finddif(dataset1, dataset2, buckets, varlist, options)
    if scores == 0:
        if not is_ro:
            return json.dumps(results_json = {'success': False, 'message': 'A variable has no data',
                                              'message_type': 'info', 'data': []})
    ordered_scores = findhighest(scores)
    # print(ordered_scores)

    allvarids = [var['var_id'] for var in varlist]

    results = []
    for i in range(0, len(ordered_scores)):
        indexes = ordered_scores[i][0].split('-')
        indexes = [int(indx) for indx in indexes]
        type = varlist[indexes[0]]['var_subtype']
        if type in ['decimal', 'numeric', 'scalar']:
            id_pos = indexes[0]
            var_id = allvarids[id_pos]
            feature = buckets[indexes[0]][indexes[1]]
            score = ordered_scores[i][1] * 100
            results.append((var_id, feature, score))
        elif type in ['text']:
            id_pos = indexes[0]
            results.append((allvarids[id_pos], '0-0', 0.0))
        else:
            id_pos = indexes[0]
            var_id = allvarids[id_pos]
            feature = options[indexes[0]][indexes[1]]
            score = ordered_scores[i][1]*100
            results.append((var_id, feature, score))

    print(results)

    if not is_ro:
        try:
            results_json = {'success': True, 'message': None, 'message_type': None, 'data': []}
            for var_id, feature, score in results:
                if isinstance(feature, list):
                    results_json['data'].append({'v': var_id,
                                                 'ov': None,
                                                 'on': None,
                                                 'b': str(feature[0]) + '-' + str(feature[1]),
                                                 's': round(score, 2)})
                elif isinstance(feature, tuple):
                    results_json['data'].append({'v': var_id,
                                                 'ov': feature[0],
                                                 'on': feature[1],
                                                 'b': None,
                                                 's': round(score, 2)})
                else:
                    continue
            results_json = json.dumps(results_json)
            print(results_json)
        except:
            results_json = {'success': False, 'message': 'Unknown error', 'message_type': 'error', 'data': []}
            results_json = json.dumps(results_json)
            print(results_json)

    if is_ro:
        ### save to database ###
        for var_id, value, score in results:
            bucket = "-"
            if isinstance(value, list):
                query = """INSERT INTO segment_diff_results SET analysis_id = {analysis_id}, var_id = {var_id}, 
                           bucket = '{bucket}', score = {score}""".format(
                           analysis_id=analysis_id, var_id=var_id,
                           bucket=str(value[0])+'-'+str(value[1]), score=round(score, 2))
                cursor = cnx.cursor()
                cursor.execute(query)
                cursor.close()
            elif isinstance(value, tuple):
                query = """INSERT INTO segment_diff_results SET analysis_id = {analysis_id}, var_id = {var_id}, 
                           option_value = {option_value}, score = {score}""".format(
                           analysis_id=analysis_id, var_id=var_id, option_value=value[0], score=round(score, 2))
                cursor = cnx.cursor()
                cursor.execute(query)
                cursor.close()
            else:
                exit(1)

        cursor = cnx.cursor(dictionary=True)
        try:
            cursor.execute("""UPDATE analysis SET analysis_status = 'done',
                                    error = NULL,
                                    modified = '{current_time}' 
                                    WHERE id = {current_id}""".format(current_time=now, current_id=analysis_id))
        except:
            exit(1)
        cnx.commit()
        cursor.close()
        cnx.close()
        exit(0)

    if not is_ro:
        return results_json
    else:
        return None


def mymin(mylist):
    return min(i for i in mylist if i is not None)


def mymax(mylist):
    return max(i for i in mylist if i is not None)


def getdata(org_id, survey_id, ro_id, segment_ids, analysis_id):
    if org_id == 0 or survey_id == 0 or ro_id == 0 or segment_ids == "":
        exit(1)

    filters = []

    # print("---")

    segment_ids_list = segment_ids.split(",")
    # print(len(segment_ids_list))

    if len(segment_ids_list) == 1:
        # new
        cursor = cnx.cursor(dictionary=True, buffered=True)
        try:
            cursor.callproc('GetSurveyDataIS', [org_id, survey_id, int(segment_ids_list[0]), 1])
        except mysql.connector.Error as error:
            print(error)
            cursor_err = cnx.cursor()
            cnx.commit()
            cursor_err.close()
            cnx.close()
            exit(1)
        for i in cursor.stored_results():
            columns1 = i.column_names
            dataset1 = i.fetchall()

        # print(dataset1)
        # print(columns1)

        # new
        cursor = cnx.cursor(dictionary=True, buffered=True)
        try:
            cursor.callproc('GetSurveyDataES', [org_id, survey_id, int(segment_ids_list[0]), 1])
        except mysql.connector.Error as error:
            print(error)
            cursor_err = cnx.cursor()
            cnx.commit()
            cursor_err.close()
            cnx.close()
            exit(1)
        for i in cursor.stored_results():
            columns2 = i.column_names
            dataset2 = i.fetchall()

    elif len(segment_ids_list) == 2:
        # new
        cursor = cnx.cursor(dictionary=True, buffered=True)
        try:
            cursor.callproc('GetSurveyDataIS', [org_id, survey_id, int(segment_ids_list[0]), 1])
        except mysql.connector.Error as error:
            print(error)
            cursor_err = cnx.cursor()
            cnx.commit()
            cursor_err.close()
            cnx.close()
            exit(1)
        for i in cursor.stored_results():
            columns1 = i.column_names
            dataset1 = i.fetchall()

        # new
        cursor = cnx.cursor(dictionary=True, buffered=True)
        try:
            cursor.callproc('GetSurveyDataIS', [org_id, survey_id, int(segment_ids_list[1]), 1])
        except mysql.connector.Error as error:
            print(error)
            cursor_err = cnx.cursor()
            cnx.commit()
            cursor_err.close()
            cnx.close()
            exit(1)
        for i in cursor.stored_results():
            columns2 = i.column_names
            dataset2 = i.fetchall()

    else:
        exit(1)

    ###
    dicts1 = []
    dicts2 = []

    for point in dataset1:
        newdict = {}
        for i in range(0, len(columns1)):
            newdict[columns1[i]] = point[i]
        dicts1.append(newdict)
    dataset1 = dicts1

    for point in dataset2:
        newdict = {}
        for i in range(0, len(columns2)):
            newdict[columns2[i]] = point[i]
        dicts2.append(newdict)
    dataset2 = dicts2

    dataset1_cleaned = []
    for point in dataset1:
        point_cleaned = {}
        for key in point.keys():
            if re.match("^[0-9]+$", key):
                point_cleaned[key] = point[key]
        dataset1_cleaned.append(point_cleaned)
    dataset1 = dataset1_cleaned
    # print(dataset1)

    dataset2_cleaned = []
    for point in dataset2:
        point_cleaned = {}
        for key in point.keys():
            if re.match("^[0-9]+$", key):
                point_cleaned[key] = point[key]
        dataset2_cleaned.append(point_cleaned)
    dataset2 = dataset2_cleaned
    # print(dataset2)

    # print(dataset1)
    # print(dataset2)

    cursor = cnx.cursor(dictionary=True, buffered=True)
    try:
        cursor.execute("""SELECT var_id, var_type, var_subtype, var_name FROM var_list WHERE org_id = {org_id} AND
                          survey_id = {survey_id} AND var_subtype
                          IN('single_choice', 'multiple_choice', 'numeric', 'scalar', 'decimal');""".format(
                          org_id=org_id, survey_id=survey_id))
    except:
        cnx.close()
        exit(1)
    varlist = cursor.fetchall()
    cursor.close()

    # print(varlist)

    options = []
    for var in varlist:
        if var['var_subtype'] in ['single_choice', 'multiple_choice']:
            cursor = cnx.cursor(dictionary=True, buffered=True)
            cursor.callproc('GetVariableOptions', [var['var_id']])
            for i in cursor.stored_results():
                options.append(i.fetchall())
            cursor.close()
        else:
            options.append([])

    return dataset1, dataset2, varlist, options, filters


def create_buckets(dataset, varlist):
    buckets = []
    if dataset == []:
        print(json.dumps({'success': False, 'message': 'One of the datasets has no data',
                          'message_type': 'info', 'data': []}))
        exit(0)
    keys = list(dataset[0])
    # print(keys)
    # print(varlist)
    for i in range(0, len(varlist)):
        print(varlist[i])
        type = varlist[i]['var_subtype']
        if type in ['numeric', 'decimal']:
            values = []
            for point in dataset:
                values.append(point[str(varlist[i]['var_id'])])
            if [i for i in values if i is not None] == []:
                print(json.dumps({'success': False, 'message': 'Variable ' + str(varlist[i]['var_name']) + ' has no data',
                                                'message_type': 'info', 'data': []}))
                exit(0)
            intervals = bucketize(mymin(values), mymax(values), 't')
            buckets.append([[float(limit) for limit in interval.split('-')] for interval in intervals])
        elif type in ['scalar']:
            values = []
            for point in dataset:
                # values.append(point[keys[i]])
                values.append(point[str(varlist[i]['var_id'])])
            print(values)
            if [i for i in values if i is not None] == []:
                print(json.dumps(
                    {'success': False, 'message': 'Variable ' + str(varlist[i]['var_name']) + ' has no data',
                     'message_type': 'info', 'data': []}))
                exit(0)
            print("---")
            intervals = bucketize(mymin(values), mymax(values), 'f')
            print("---")
            buckets.append([[int(limit) for limit in interval.split('-')] for interval in intervals])
        else:
            buckets.append([])
    return buckets


def finddif(dataset, filtered, buckets, varlist, options):
    scores = []
    if dataset == [] or filtered == []:
        exit(1)
    keys = list(dataset[0])
    """
    for point in dataset:
        print(list(point.values()))
    """
    for i in range(0, len(varlist)):
        temp = []
        for point in dataset:
            temp.append(point[str(varlist[i]['var_id'])])
        if all(v is None for v in temp):
            return 0

    for i in range(0, len(varlist)):
        type = varlist[i]['var_subtype']
        if type in ['numeric', 'decimal']:
            scores_thisvar = []
            bucketcount = len(buckets[i])
            # format: [dataset_within, dataset_without, filtered_within, filtered_without]
            for interval in buckets[i]:
                dataset_within, dataset_without, filtered_within, filtered_without = 0, 0, 0, 0
                for var in dataset:
                    # value = var[keys[i]]
                    value = var[str(varlist[i]['var_id'])]
                    if value != None and value >= interval[0] and value <= interval[1]:
                        dataset_within += 1
                    else:
                        dataset_without += 1
                for var in filtered:
                    # value = var[keys[i]]
                    value = var[str(varlist[i]['var_id'])]
                    if value != None and value >= interval[0] and value <= interval[1]:
                        filtered_within += 1
                    else:
                        filtered_without += 1
                beta_dataset = beta.rvs(dataset_within + 10, dataset_without + 10, size=100)
                beta_filtered = beta.rvs(filtered_within + 10, filtered_without + 10, size=100)
                differences = []
                for j in range(0, 100):
                    differences.append(abs(beta_dataset[j] - beta_filtered[j]))
                difference = np.mean(differences)
                scores_thisvar.append(difference)
            scores.append(scores_thisvar)
        elif type in ['scalar']:
            scores_thisvar = []
            bucketcount = len(buckets[i])
            for interval in buckets[i]:
                dataset_within, dataset_without, filtered_within, filtered_without = 0, 0, 0, 0
                for var in dataset:
                    value = var[str(varlist[i]['var_id'])]
                    if value != None and value >= interval[0] and value <= interval[1]:
                        dataset_within += 1
                    else:
                        dataset_without += 1
                for var in filtered:
                    value = var[str(varlist[i]['var_id'])]
                    if value != None and value >= interval[0] and value <= interval[1]:
                        filtered_within += 1
                    else:
                        filtered_without += 1
                beta_dataset = beta.rvs(dataset_within + 10, dataset_without + 10, size=100)
                beta_filtered = beta.rvs(filtered_within + 10, filtered_without + 10, size=100)
                differences = []
                for j in range(0, 100):
                    differences.append(abs(beta_dataset[j] - beta_filtered[j]))
                # print(np.var(differences))
                difference = np.mean(differences)
                scores_thisvar.append(difference)
            scores.append(scores_thisvar)
        elif type in ['single_choice']:
            scores_thisvar = []
            varops = options[i]
            optcount = len(varops)
            for optval, optnam in varops:
                dataset_equal, dataset_inequal, filtered_equal, filtered_inequal = 0, 0, 0, 0
                for var in dataset:
                    # value = var[keys[i]]
                    value = var[str(varlist[i]['var_id'])]
                    if value == optval:
                        dataset_equal += 1
                    else:
                        dataset_inequal += 1
                for var in filtered:
                    # value = var[keys[i]]
                    value = var[str(varlist[i]['var_id'])]
                    if value == optval:
                        filtered_equal += 1
                    else:
                        filtered_inequal += 1
                beta_dataset = beta.rvs(dataset_equal + 10, dataset_inequal + 10, size=100)
                beta_filtered = beta.rvs(filtered_equal + 10, filtered_inequal + 10, size=100)
                differences = []
                for j in range(0, 100):
                    differences.append(abs(beta_dataset[j] - beta_filtered[j]))
                # print(np.var(differences))
                difference = np.mean(differences)
                scores_thisvar.append(difference)
            scores.append(scores_thisvar)
        elif type in ['multiple_choice']:
            scores_thisvar = []
            varops = options[i]
            optcount = len(varops)
            for optval, optnam in varops:
                dataset_equal, dataset_inequal, filtered_equal, filtered_inequal = 0, 0, 0, 0
                for var in dataset:
                    # value = var[keys[i]]
                    value = var[str(varlist[i]['var_id'])]
                    if value is None:
                        dataset_inequal += 1
                    else:
                        if str(optval) in value.split(','):
                            dataset_equal += 1
                        else:
                            dataset_inequal += 1
                for var in filtered:
                    # value = var[keys[i]]
                    value = var[str(varlist[i]['var_id'])]
                    if value is None:
                        filtered_inequal += 1
                    else:
                        if str(optval) in value.split(','):
                            filtered_equal += 1
                        else:
                            filtered_inequal += 1
                beta_dataset = beta.rvs(dataset_equal + 10, dataset_inequal + 10, size=100)
                beta_filtered = beta.rvs(filtered_equal + 10, filtered_inequal + 10, size=100)
                differences = []
                for j in range(0, 100):
                    differences.append(abs(beta_dataset[j] - beta_filtered[j]))
                # print(np.var(differences))
                difference = np.mean(differences)
                scores_thisvar.append(difference)
            scores.append(scores_thisvar)
        else:
            scores.append([0])
    return scores


def findhighest(scores):
    # print(scores)
    serialized = {}
    counter = 0
    for i in range(0, len(scores)):
        for j in range(0, len(scores[i])):
            indx = str(i) + '-' + str(j)
            serialized[indx] = scores[i][j]
    serialized = sorted(serialized.items(), key=lambda x: x[1], reverse=True)
    return serialized


if __name__ == "__main__":
    if test:
        # org_id, survey_id, ro_id, segment_ids, analysis_id = 1, 131, 1, "8,9", None
        # org_id, survey_id, ro_id, segment_ids, analysis_id = 1, 131, 1, "8,9", 379
        # org_id, survey_id, ro_id, segment_ids, analysis_id = 1, 131, 1, "8", 379
        org_id, survey_id, ro_id, segment_ids, analysis_id = 1, 238, 1470, "92,94", None
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument('--org', '-o', help="", type=int, default=0)
        parser.add_argument('--survey', '-s', help="", type=int, default=0)
        parser.add_argument('--ro', '-r', help="", type=int, default=0)
        parser.add_argument('--segments', '-t', help="", type=str, default="")
        parser.add_argument('--analysis', '-a', help="", type=int, default=None)
        args = parser.parse_args()
        org_id = args.org
        survey_id = args.survey
        ro_id = args.ro
        segment_ids = args.segments
        analysis_id = args.analysis

    main(org_id, survey_id, ro_id, segment_ids, analysis_id)

