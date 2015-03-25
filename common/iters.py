

# def categories(relmon_request, only_names=None, skip_names=None):
#     for category in relmon_request["categories"]:
#         if (skip_names is not None and
#             category["name"] in skip_names):
#             # then:
#             continue
#         if (only_names is not None):
#             if (category["name"] in only_names):
#                 yield category
#             continue
#         yield category


# source - category iterator
def samples(categories, only_statuses=None, skip_statuses=None):
    for category in categories:
        for sample_list in category["lists"].itervalues():
            for sample in sample_list:
                if (skip_statuses is not None and
                    sample["status"] in skip_statuses):
                    # then:
                    continue
                if (only_statuses is not None):
                    if (sample["status"] in only_statuses):
                        yield sample
                    continue
                yield sample
