# -*- coding: utf-8 -*-

from core import TurfConnect
# import info_modificators




query = "SELECT * FROM cachedate WHERE id='184768790814'"

turf = TurfConnect()

"""
for k in ks:
    print("column: {} | {}".format(k, type(r[k])))
"""

ncourse = '2289455145'

daddres = "/home/horacio/Documents/turf/csv_files"
fname = "test"

hs_columns = ['cheval', 'age', 'sexe', 'jockey']
gb_columns = ['jour', 'hippo', 'dist', 'partant']

turf.write_course_to_csv(numcourse=ncourse, dir_address=daddres,
                         file_name=fname, hs_columns=hs_columns, gb_columns=gb_columns)

turf.write_course_to_csv_all_by_type(dir_address=daddres, file_name=(fname+"02"), typec="Plat", hs_columns=hs_columns, gb_columns=gb_columns)
