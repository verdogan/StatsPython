import json
import argparse
import numpy as np
import collections
from kmodes.kmodes import KModes
from sklearn.cluster import KMeans
from scipy.stats import pointbiserialr
from connection import connect, currtime


test = False


def main(org_id, survey_id, variable_ids):
    x, options = getdata(org_id, survey_id, variable_ids)
    b, options, medians = binarize(x, options)
    # colinearity(b)
    cluster_count = optclustercount(b)
    labels, member_counts = cluster(b, cluster_count)
    features = featurize(b, labels, options, cluster_count)
    prominent_features_json = jsonize(variable_ids, features, options, medians, member_counts)
    return print(prominent_features_json)


def getdata(org_id, survey_id, variable_ids):
    if org_id == 0 or survey_id == 0 or variable_ids == "":
        exit(1)

    variable_ids = variable_ids.split(",")
    # checkprint(variable_ids)

    cnx = connect()
    now = currtime()

    # checkprint("start")

    counter = 0
    data_raw = []
    for variable_id in variable_ids:
        if not int(variable_id) == 2846:
            continue
        print(counter)
        counter += 1
        cursor = cnx.cursor(dictionary=True, buffered=True)
        try:
            cursor.callproc('GetVariableData', [variable_id, org_id, survey_id])
        except:
            cursor_err = cnx.cursor()
            cnx.commit()
            cursor_err.close()
            cnx.close()
            exit(1)
        for i in cursor.stored_results():
            fetched = i.fetchall()
            print(fetched)
            fetched = [('0',) if element[0] is None else element for element in fetched]
            data_raw.append(fetched)
        cursor.close()

    exit(1)

    options = []
    for variable_id in variable_ids:
        print(counter)
        counter += 1
        cursor = cnx.cursor(dictionary=True, buffered=True)
        if True:
            try:
                cursor.callproc('GetVariableOptions', [variable_id])
            except:
                cursor_err = cnx.cursor()
                cnx.commit()
                cursor_err.close()
                cnx.close()
                exit(1)
            for i in cursor.stored_results():
                options.append(i.fetchall())
        cursor.close()

    # checkprint(options)

    cnx.close()
    # for variable in data_raw:
        # checkprint(variable)
    # checkprint(options)
    return data_raw, options


def binarize(x, options):
    medians = []
    x = np.array(x)
    x = x[:,:,0]
    x = x.transpose()
    # checkprint(x)
    for column in x.T:
        rowcount = len(column)
        break
    colcount = 0
    for question in options:
        if question == []:
            colcount += 1
        else:
            colcount += len(question)
    b = np.zeros((rowcount, colcount), dtype=np.int)
    bcol_counter = 0
    for column in range(len(x.T)):
        if options[column] == []:
            median = np.median(x.T[column].astype(float))
            medians.append(median)
            for row in range(len(x.T[column])):
                if float(x[row, column]) > median:
                    b[row, bcol_counter] = 1
                else:
                    b[row, bcol_counter] = 0
            # for element in b.T[bcol_counter]:
                # checkprint(element)
            bcol_counter += 1
        else:
            medians.append(None)
            for option_code, option_name in options[column]:
                for row in range(len(x.T[column])):
                    value = x[row, column]
                    choices = [int(x.strip()) for x in value.split(',')]
                    # checkprint(choices)
                    if int(option_code) in choices:
                        b[row, bcol_counter] = 1
                        # checkprint(1)
                    else:
                        b[row, bcol_counter] = 0
                        # checkprint(0)
                bcol_counter += 1
    return b, options, medians


def colinearity(b):
    for i in range(len(b.T)):
        for j in range(len(b.T)):
            if i != j:
                corr, p = pointbiserialr(b.T[i], b.T[j])
    return True


def validate(b):
    scores = []
    K = range(2, 6)
    for k in K:
        km = KMeans(n_clusters=k)
        km = km.fit(b)
        scores.append(km.inertia_ * float(k))
    # checkprint("scores:")
    # checkprint(scores)
    return scores


def optclustercount(b):
    scores = validate(b)
    optimal = scores.index(max(scores)) + 2
    # checkprint("optimal")
    # checkprint(optimal)
    if optimal in [2, 3, 4, 5]:
        print(optimal)
        return optimal
    else:
        return 3


def cluster(b, cluster_count):
    kmodes_cao = KModes(n_clusters=cluster_count, init='Cao', verbose=1)
    labels = kmodes_cao.fit_predict(b)
    # checkprint(collections.Counter(labels))
    member_counts = collections.Counter(labels)
    return labels, member_counts


def featurize(b, labels, options, cluster_count):
    binaries = []
    cluster_pairs = []
    feature_scores = []
    all_features = []
    for cluster_label in range(0, cluster_count):
        is_included = [True if label == cluster_label else False for label in labels]
        ingroup = b[np.array(is_included), :]
        outgroup = b[~np.array(is_included), :]
        cluster_pair = [ingroup, outgroup]
        cluster_pairs.append(cluster_pair)
    counter = 0
    for cluster_pair in cluster_pairs:
        feature_scores_this_cluster = []
        for i in range(len(cluster_pair[0].T)):
            counts_this_cluster = collections.Counter(cluster_pair[0].T[i])
            counts_comp_cluster = collections.Counter(cluster_pair[1].T[i])
            total_this_cluster = counts_this_cluster[0] + counts_this_cluster[1]
            total_comp_cluster = counts_comp_cluster[0] + counts_comp_cluster[1]
            feature_scores_this_cluster.append(counts_this_cluster[1] / total_this_cluster -
                                               counts_comp_cluster[1] / total_comp_cluster)
            counter += 1
        feature_scores.append(feature_scores_this_cluster)
    important_features = []
    for cluster_scores in feature_scores:
        absolute_values = [abs(score) for score in cluster_scores]
        prominent_features = sorted(range(len(absolute_values)), key=lambda i: absolute_values[i], reverse=True)[:3]
        prominent_features_w_sgn = [x+1 if cluster_scores[x] > 0 else -1 * (x+1) for x in prominent_features]
        # checkprint(prominent_features)
        # checkprint(prominent_features_w_sgn)
        all_features.append(prominent_features_w_sgn)
    return all_features


def jsonize(variable_ids, features, options, medians, member_counts):
    member_total = sum(member_counts.values())
    print(member_counts)
    variable_ids = variable_ids.split(",")
    output = []
    segno = -1
    for cluster_features in features:
        segno += 1
        prominent_features = []
        for feature in cluster_features:
            id = 0
            operator = ""
            value = 0
            option_counter = 1
            id_counter = 0
            for variable in options:
                if variable == []:
                    if abs(feature) == option_counter:
                        id = int(variable_ids[id_counter])
                        value = medians[id_counter]
                        if feature > 0:
                            operator = ">"
                        else:
                            operator = "<="
                    option_counter += 1
                else:
                    for option_number, option_name in variable:
                        if abs(feature) == option_counter:
                            if abs(feature) == option_counter:
                                id = int(variable_ids[id_counter])
                                value = int(option_number)
                                if feature > 0:
                                    operator = "="
                                else:
                                    operator = "!="
                        option_counter += 1
                id_counter += 1
            prominent_features.append({"i": id, "o": operator, "v": value,
                                       "n": member_counts[segno], "p": int(member_counts[segno]*100/member_total)+1})
        output.append(prominent_features)
    edited_output = []
    for cluster in output:
        new_cluster = []
        var_ids = []
        for feature in cluster:
            if feature['i'] not in var_ids:
                new_cluster.append(feature)
            var_ids.append(feature['i'])
        edited_output.append(new_cluster)
    edited_output = json.dumps(edited_output)
    return edited_output


if __name__ == "__main__":
    if test:
        org_id = 1
        survey_id = 165
        variable_ids = ""
        for i in range(2742, 2744):
            variable_ids += str(i)
            variable_ids += ","
        variable_ids = variable_ids[:-1]
        variable_ids = allvars165
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument('--org', '-o', help="", type=int, default=0)
        parser.add_argument('--survey', '-s', help="", type=int, default=0)
        parser.add_argument('--variables', '-v', help="", type=str, default="")
        args = parser.parse_args()
        org_id = args.org
        survey_id = args.survey
        variable_ids = args.variables

    print(main(org_id, survey_id, variable_ids))