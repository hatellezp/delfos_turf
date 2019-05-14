# -*- coding: utf-8 -*-

from core import TurfConnect
# import info_modificators

COLUMNS = ['id', 'comp', 'jour', 'hippo', 'numcourse', 'cl', 'dist', 'partant',
           'typec', 'cheque', 'numero', 'cheval', 'sexe', 'age', 'cotedirect',
           'coteprob', 'recence', 'ecurie', 'distpoids', 'ecar', 'redkm',
           'redkmInt', 'handiecords', 'corde', 'defoeil', 'recul', 'gains',
           'musiquept', 'musiqueche', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6',
           'jockey', 'musiquejoc', 'montesdujockeyjour', 'couruejockeyjour',
           'victoirejockeyjour', 'entraineur', 'musiqueent',
           'monteentraineurjour', 'courueentraineurjour',
           'victoireentraineurjour', 'coursescheval', 'victoirescheval',
           'placescheval', 'coursesentraineur', 'victoiresentraineur',
           'placeentraineur', 'coursesjockey', 'victoiresjockey', 'placejockey',
           'dernierhippo', 'dernierealloc', 'derniernbpartants', 'dernieredist',
           'derniereplace', 'dernierecote', 'dernierJoc', 'dernierEnt',
           'dernierProp', 'proprietaire', 'nbcoursepropjour', 'europ', 'amat',
           'arrive', 'txrecl', 'pays', 'meteo', 'lice', 'natpis', 'pistegp',
           'prix', 'poidmont', 'pourcVictJock', 'pourcPlaceJock',
           'pourcVictCheval', 'pourcPlaceCheval', 'pourcVictEnt',
           'pourcPlaceEnt', 'pourcVictEntHippo', 'pourcVictJockHippo',
           'pourcPlaceEntHippo', 'pourcPlaceJockHippo', 'pourcVictChevalHippo',
           'pourcPlaceChevalHippo', 'nbrCourseJockHippo', 'nbrCourseEntHippo',
           'nbrCourseChevalHippo', 'nbCourseCouple', 'nbVictCouple',
           'nbPlaceCouple', 'TxVictCouple', 'TxPlaceCouple',
           'nbCourseCoupleHippo', 'nbVictCoupleHippo', 'nbPlaceCoupleHippo',
           'TxVictCoupleHippo', 'TxPlaceCoupleHippo', 'pere', 'mere',
           'peremere', 'coteleturf', 'commen', 'gainsCarriere',
           'gainsVictoires', 'gainsPlace', 'gainsAnneeEnCours',
           'gainsAnneePrecedente', 'jumentPleine', 'engagement',
           'handicapDistance', 'handicapPoids', 'indicateurInedit', 'tempstot',
           'vha', 'recordG', 'txreclam', 'createdat', 'updatedat',
           'rangTxVictJock', 'rangTxVictCheval', 'rangTxVictEnt',
           'rangTxPlaceJock', 'rangTxPlaceCheval', 'rangTxPlaceEnt',
           'rangRecordG'
           ]

# not finished here
HS_COLUMNS = []
GB_COLUMNS = ["id", "comp", "jour", "hippo", "numcourse", "dist", "partant",
              "typec", "pays", "meteo", "prix", ]

query = "SELECT * FROM cachedate WHERE id='184768790814'"

turf = TurfConnect()

daddres = "/home/horacio/Documents/turf/delfos_turf/csv_files"

hs_columns = ['cheval', 'age', 'sexe', 'jockey']
gb_columns = ['jour', 'hippo', 'dist', 'partant']


# turf.write_course_to_csv(numcourse=ncourse, dir_address=daddres, file_name=fname, hs_columns=hs_columns, gb_columns=gb_columns)

turf.write_course_to_csv_all_by_type(dir_address=daddres, typec="Plat",
                                     hs_columns=hs_columns,
                                     gb_columns=gb_columns,
                                     all_out=False,
                                     id_use=False,
                                     all_typec=True)

"""
turf.update_to_latest_csv_file(dir_address=daddres,
                               hs_columns=hs_columns,
                               gb_columns=gb_columns)
"""
