# -*- coding: utf-8 -*-

from core import TurfConnect
# import info_modificators


query = "SELECT * FROM cachedate WHERE id='184768790814'"

turf = TurfConnect()

"""
for k in ks:
    print("column: {} | {}".format(k, type(r[k])))
"""


list_dict = turf.complete_course('2289455145')

print(len(list_dict))
i = list_dict[4]

for key in i.keys():
    print("{}:{}".format(key, i[key]))
print("="*100)


