# -*- coding: utf-8 -*-

import mysql.connector
import datetime
import copy
import csv

##############################################################################
##############################################################################
"""
This file contains the necessary to connect to the database and create the
needed files to machine learning.
"""

# Obs : the string that the database returns are unicode, take that in account.
# UPDATE: In python3, we can drop the unicode problems
# COLUMNS has 129 elemtents
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
##############################################################################
##############################################################################



class TurfConnect:
    """
    This class should contains almost everythin present in this file.
    """

    def __init__(self):

        #######################################################################
        # the parameters to connect to the database are encoded in
        # the machine, not in the cloud or github
        file_param = csv.DictReader(open("../parameters.csv"))
        param = None
        for row in file_param:
            param = row

        self.host = param['host']
        self.user = param['username']
        self.passwd = param['passwd']
        self.database = param['database']
        self.columns = COLUMNS
        self.normal = int(param['normal'])
        #######################################################################

        #######################################################################
        print("="*100)
        print(("Attempt to connect to database with parameters:\n    "
               "-host={}\n    -user={}\n    -passwd={}\n    -database={}").
              format(self.host, self.user, self.passwd, self.database))

        # connecting to the database
        try:
            mydb = mysql.connector.connect(
                host=self.host,
                user=self.user,
                passwd=self.passwd,
                database=self.database
            )
        except:
            print("Error while conncting to database")

        print("Connection made.\n")
        print("="*100)
        self.db = mydb
        self.cursor = mydb.cursor(dictionary=True)  # dictionnary is the good
                                                    # way
        #######################################################################

        #######################################################################
        """
        This idea of making all races the same size is being tested.
        I'm not sure how to manage inconsisten size data.

        The dummy horses are not complete. They have fields missing.
        Those fields are updated when some race has to be completed.
        """

        print("="*100)
        print("Creating dummy horses...")
        # the connection has been made, we make the dummy horses here
        list_dummy_horses = []
        list_dummy_horse_names = []
        list_dummy_horse_jockey = []
        list_dummy_horse_entraineur = []
        list_dummy_horse_proprietaire = []
        for i in range(40):
            name = 'dummy' + str(i)
            dummy = TurfConnect.create_dummy_horse(name)

            list_dummy_horses.append(dummy)

            # horse names
            list_dummy_horse_names.append(name)

            # dummy jockey names
            list_dummy_horse_jockey.append(dummy['jockey'])
            list_dummy_horse_jockey.append(dummy['dernierJoc'])

            list_dummy_horse_entraineur.append(dummy['entraineur'])
            list_dummy_horse_entraineur.append(dummy['dernierEnt'])

            list_dummy_horse_proprietaire.append(dummy['proprietaire'])
            list_dummy_horse_proprietaire.append(dummy['dernierProp'])

        self.dummy_horses = list_dummy_horses
        print("Done. {} dummy horses created.\n".format(len(self.dummy_horses)))
        print("="*100)
        #######################################################################

        #######################################################################
        """
        This part is the list of global information that need to be maped to
        some constant and simpler values.
        """
        print("="*100)
        print("Getting list of race types...")
        # this form take the information directly from the database
        self.type_course = self.make_list_arg('typec')
        # print(self.type_course)
        print("Done.\n")

        print("Getting list of races...")
        self.courses = self.make_list_arg('numcourse')
        # print(self.courses)
        print("Done.\n")

        print("Getting list of horse names...")
        # pour le chevaux c'est un peu plus long, il faut ajouter peres et
        # meres aussi
        # j'ai changé mais maintenant ça sera pas si complet
        meres = self.make_list_arg('mere')
        peres = self.make_list_arg('pere')
        self.chevaux = self.make_list_arg('cheval')
        meres.extend(peres)
        self.chevaux.extend(meres)
        self.chevaux.extend(list_dummy_horse_names) # verify the correcteness
                                                    # of this after
        print("Done.\n")

        print("Getting list of hippodromes...")
        self.hippo = self.make_list_arg('hippo')
        self.hippo.append("dummy_hippo")
        # print(self.hippo)
        print("Done.\n")

        print("Getting list of ecuries...")
        # ecurie + dummyecu + bijection
        self.ecuries = self.make_list_arg('ecurie')
        self.chevaux.append("dummy_ecurie")
        print("Done.\n")

        print("Getting list of jockey names...")
        # jockey + dernierJoc + dummyjoc + bijection
        dernierJocs = self.make_list_arg('dernierJoc')
        self.jockeys = self.make_list_arg('jockey')
        self.jockeys.extend(dernierJocs)
        self.jockeys.extend(list_dummy_horse_jockey)
        print("Done.\n")

        print("Getting list of trainer names...")
        # entraineur + dernierEnt + dummyent + bijection
        dernierEnts = self.make_list_arg('dernierEnt')
        self.trainers = self.make_list_arg('entraineur')
        self.trainers.extend(dernierEnts)
        self.trainers.extend(list_dummy_horse_entraineur)
        print("Done.\n")

        print("Getting list of owner names...")
        # proprietaire + dernierProp + dummyprop + bijection
        dernierProps = self.make_list_arg('dernierProp')
        self.owners = self.make_list_arg('proprietaire')
        self.owners.extend(dernierProps)
        self.owners.extend(list_dummy_horse_proprietaire)
        print("Done.\n")
        print("="*100)

        print("="*100)
        print("Creating bijections...")
        self.hippo_bijection = TurfConnect.create_bijcetion(self.hippo)
        print("    hippodromes")
        self.chevaux_bijection = TurfConnect.create_bijcetion(self.chevaux)
        print("    horses")
        self.ecuries_bijection = TurfConnect.create_bijcetion(self.ecuries)
        print("    ecuries")
        self.jockeys_bijection = TurfConnect.create_bijcetion(self.jockeys)
        print("    jockeys")
        self.trainers_bijection = TurfConnect.create_bijcetion(self.trainers)
        print("    trainers")
        self.owners = TurfConnect.create_bijcetion(self.owners)
        print("    owners")
        print("Done.\n")
        print("="*100)

        print("="*100)
        print("Initialization done.")
        #######################################################################

        #######################################################################
        # stupid test to see if the database can answer the simplest query
        if self.test():
            print("Dummy test passed.")
        print("="*100)

    # dummy function to test the connection with the database
    def test(self):
        id = '22747426921'
        q = "SELECT * FROM cachedate WHERE id={}".format(id)
        self.cursor.execute(q)
        res = self.cursor.fetchall()[0]
        return not(res is None)

    # I wrapped the 'cursor.execute' method in something more simple.
    # Because performance is not an issue (for the moment), I take
    # the whole return value of the query.
    def pass_query(self, q):
        self.cursor.execute(q)
        return self.cursor.fetchall()

    ###########################################################################
    # Group of function to guaranteed safety of the queries.
    def is_valid_course(self, ncourse):
        # self.courses is now a list of integers
        return int(ncourse) in self.courses

    def is_valid_horse(self, nhorse):
        return nhorse in self.chevaux

    def is_new_horse(self, nhorse):
        return not(self.is_valid_horse(nhorse))

    def is_valid_hippo(self, nhippo):
        return nhippo in self.hippo

    def is_new_hippo(self, nhippo):
        return not(self.is_valid_horse(nhippo))
    ###########################################################################

    ###########################################################################
    # more crafty function to get information from the database
    def get_specific_course(self, ncourse):
        if self.is_valid_course(ncourse):
            query = "SELECT * FROM cachedate WHERE numcourse={};".\
                format(ncourse)
            # this is list, with each element a row for a horse in the
            # race
            return self.pass_query(query)

    def get_type_of_course_from_numcourse(self, ncourse):
        if self.is_valid_course(ncourse):
            res = self.get_specific_course(ncourse)[0]
            return res['typec']
    ############################################################################

    ###########################################################################
    # Group of helper functions.

    # For columns like 'cheval', 'typec'... this wrap all types in a list
    # and returns it.
    def make_list_arg(self, column):
        l = [] # list to return

        # query definition and execution
        query = "SELECT {} FROM cachedate GROUP BY {}".format(column, column)
        res = self.pass_query(query)

        # appending resutls
        for r in res:
            l.append(r[column])
        return l

    # This returns all columns that exist in the database excluding those in
    # 'ex_columns'.
    def get_columns_but_some(self, ex_columns):
        return [x for x in self.columns if not(x in ex_columns)]

    # This function returns a query answer where only the wanted columns are
    # present.
    # 'cs' is a list of columns
    # 'excluded' says if the columns in 'cs' are to be excluded or put in the
    #   query
    # 'where_condition' is self-explanatory
    def define_select_columns_query(self, cs, excluded=False, where_condition=None):
        if excluded:
            col_to_take = self.get_columns_but_some(cs)
        else:
            col_to_take = cs

        col_to_take_str = str(col_to_take[0])
        for col in col_to_take[1:]:
            col_to_take_str += "," + str(col)

        if where_condition is None:
            query = "SELECT {} FROM cachedate".format(col_to_take_str)
        else:
            query = "SELECT {} FROM cachedate WHERE {}".\
                format(col_to_take_str, where_condition)
        return query

    # A pretty print function to present the 'cursor' answer that is a
    # dictionary.
    def present_dict_answer(self, list_dd):
        res = ""
        for dd in list_dd:
            i = 0
            for key in dd.keys():
                value = dd[key]
                # we need to be careful with the encoding of strings...
                if isinstance(value, str) or isinstance(value, unicode):
                    value = value.encode('utf-8')
                aa = "{}- {}: {},\n".format(str(i), key, value)
                res += aa
                i += 1
            res += "\n"
        return res
    ###########################################################################

    ###########################################################################
    # This part is more important. Here the vectors that will be written to
    # file for machine learning are defined.

    # This function completes a list of dict horses to 'self.normal' size.
    # It will take a race number 'ncourse' and complete the number of horses
    # with dummy horses. Those dummy horses are updated with information of
    # the actual race.
    def complete_course(self, ncourse):
        if self.is_valid_course(ncourse):
            list_dict = self.get_specific_course(ncourse)
            length = len(list_dict)
            diff = self.normal - length

            # this helps for corde position
            course_type = list_dict[0]['typec']

            # base dummy horse
            base_name = "dummy"
            base_position = length + 1
            base_corde_position = length + 1

            first_horse = list_dict[0]

            for i in range(diff):
                position = base_position + i
                corde_position = 0
                if course_type == 'Plat': # this test works even in unicode???
                    corde_position = base_corde_position + i

                dummy = self.dummy_horses[i]
                TurfConnect.update_dummy_horse(dummy, first_horse, position,
                                        corde_position)
                list_dict.append(dummy)

            return list_dict
        else:
            print("ERROR: INVALID COURSE!!")


    # this is one of the most importtant function, it will build two vectors for
    # learning from a course
    # there is 'raw' at the end, this must be treated before computing of
    # storing in csv
    # this function is wrong, plainly wrong, a lot of duplicate information
    def from_course_make_vector_couple(self, course_type, numcourse):
        q = "SELECT typec FROM cachedate WHERE numcourse='{}'".\
            format(numcourse)
        res = self.pass_query(q)
        if len(res) > 0 and res[0]['typec'] == course_type:
            hippo_bijection = TurfConnect.create_bijcetion(self.hippo)
            cheval_bijection = TurfConnect.create_bijcetion(self.chevaux)
            pass


    # this function create bijection betweeen a list of something and numbers
    # usually we will us it to map not numeric values to numveric ones
    @staticmethod
    def create_bijcetion(lis):
        length = len(lis)
        def f(value, inverse=False):
            li = lis.copy()
            if not inverse:
                for i in range(length):
                    if value==li[i]:
                        return i
                return -1
            else:
                for i in li:
                    if i == li[value]:
                        return i
                return "EMPTY"
        return f

    # this has to be done manually, awesome ! I really hope all this work pays
    # the horses has to be created before each race, I'm going to make a NORMAL
    # number of dummy horses and add them when necessary
    @staticmethod
    def create_dummy_horse(name):

        # we copy any horse (in this case the first) and update only the
        # needed fields
        # dummy = copy.deepcopy(first_horse)
        dummy = dict()
        ######################################################################
        ######################################################################
        # update of forcibly different fields
        dummy['id']  = 10000000000000
        dummy['comp'] = 100000000
        dummy['hippo'] = '' # this fields depends of the race
        dummy['numcourse'] = 33
        dummy['cl'] = 0
        dummy['dist'] = 0
        dummy['partant'] = '33'
        dummy['typec'] = 'Plat'
        dummy['cheque'] = 34
        dummy['numero'] = 0 # to verify

        dummy['cheval'] = name
        dummy['sexe'] = 'M' # this two fields are arbitrary
        dummy['age'] = 6

        dummy['cotedirect'] = 0.0 # not sure about these numbers
        dummy['coteprob'] = 0.0
        dummy['recence'] = 0 # to repair and verify

        dummy['ecurie'] = 'dummy_ecurie'

        dummy['distpoids'] = ''
        dummy['ecar'] = ''
        dummy['redkm'] = "2'"
        dummy['redkmInt'] = 120000
        dummy['handiecords'] = ''
        dummy['corde'] = 0
        dummy['defoeil'] = ''
        dummy['recul'] = 0
        dummy['gains'] = ''

        dummy['musiquept'] = ''
        dummy['musiqueche'] = ''
        dummy['m1'] = 40
        dummy['m2'] = 40
        dummy['m3'] = 40
        dummy['m4'] = 40
        dummy['m5'] = 40
        dummy['m6'] = 40

        dummy['jockey'] = name + '_jockey'
        dummy['musiquejoc'] = ''
        dummy['montesdujockeyjour'] = '0'
        dummy['couruejockeyjour'] = '0'
        dummy['victoirejockeyjour'] = '0'
        dummy['entraineur'] = name + '_entraineur'
        dummy['musiqueent'] = ''
        dummy['monteentraineurjour'] = 0
        dummy['courueentraineurjour'] = 0
        dummy['victoireentraineurjour'] = 0

        dummy['coursescheval'] = 0
        dummy['victoiresceval'] = 0
        dummy['placescheval'] = 0
        dummy['coursesentraineur'] = 0
        dummy['victoiresentraineur'] = 0
        dummy['placeentraineur'] = 0
        dummy['coursesjockey'] = 0
        dummy['victoiresjockey'] = 0
        dummy['placejockey'] = 0
        dummy['dernierhippo'] = 'dummy_hippo' # good?
        dummy['dernierealloc'] = '0'
        dummy['derniernbpartants'] = '0'
        dummy['dernieredist'] = '0'
        dummy['derniereplace'] = '0'
        dummy['dernierecote'] = '0'
        dummy['dernierJoc'] = name + '_jockey'
        dummy['dernierEnt'] = name + '_entraineur'
        dummy['dernierProp'] = name + '_proprietaire'
        dummy['proprietaire'] = name + '_proprietaire'
        dummy['nbcoursepropjour'] = 0
        dummy['europ'] = ''
        dummy['amat'] = ''
        dummy['arrive'] = ''
        dummy['txrecl'] = ''
        dummy['pays'] = ''
        dummy['meteo'] = ''
        dummy['lice'] = ''
        dummy['natpis'] = ''
        dummy['pistegp'] = ''
        dummy['prix'] = ''
        dummy['poidmont'] = '60'

        # remeber that bets are not a good indicator (to verify nevertheless)
        dummy['pourcVictJock'] = 0.0
        dummy['pourcPlaceJock'] = 0.0
        dummy['pourcVictCheval'] = 0.0
        dummy['pourcPlaceCheval'] = 0.0
        dummy['pourcVictEnt'] = 0.0
        dummy['pourcPlaceEnt'] = 0.0
        dummy['pourcVictEntHippo'] = 0.0
        dummy['pourcVictJockHippo'] = 0.0
        dummy['pourcPlaceEntHippo'] = 0.0
        dummy['pourcPlaceJockHippo'] = 0.0
        dummy['pourcVictChevalHippo'] = 0.0
        dummy['pourcPlaceChevalHippo'] = 0.0

        dummy['nbrCourseJockHippo'] = 0
        dummy['nbrCourseEntHippo'] = 0
        dummy['nbrCourseChevalHippo'] = 0
        dummy['nbCourseCouple'] = 0
        dummy['nbVictCouple'] = 0
        dummy['nbPlaceCouple'] = 0
        dummy['TxVictCouple'] = 0.0
        dummy['TxPlaceCouple'] = 0.0
        dummy['nbCourseCoupleHippo'] = 0
        dummy['nbVictCoupleHippo'] = 0
        dummy['nbPlaceCoupleHippo'] = 0
        dummy['TxVictCoupleHippo'] = 0.0
        dummy['TxPlaceCoupleHippo'] = 0.0
        dummy['pere'] = 'dummy_pere'
        dummy['mere'] = 'dummy_mere'
        dummy['peremere'] = ''
        dummy['coteleturf'] = ''
        dummy['commen'] = ''
        dummy['gainsCarriere'] = 0
        dummy['gainsVictoires'] = 0
        dummy['gainsPlace'] = 0
        dummy['gainsAnneeEnCours'] = 0
        dummy['gainsAnneePrecedente'] = 0
        dummy['jumentPleine'] = 0
        dummy['engagement'] = 0
        dummy['handicapDistance'] = 0
        dummy['handicapPoids'] = 0
        dummy['indicateurInedit'] = 0
        dummy['tempstot'] = ''
        dummy['vha'] = ''
        dummy['recordG'] = None
        dummy['txreclam'] = ''
        dummy['createdat'] = datetime.datetime.now()
        dummy['updatedat'] = datetime.datetime.now()
        dummy['rangTxVictJock'] = 0
        dummy['rangTxVictCheval'] = 0
        dummy['rangTxVictEnt'] = 0
        dummy['rangTxPlaceJock'] = 0
        dummy['rangTxPlaceCheval'] = 0
        dummy['rangTxPlaceEnt'] = 0
        dummy['rangRecordG'] = 0

        # everything should give a dummy horse
        return dummy

    # here we update a dummy horse for the course needed
    @staticmethod
    def update_dummy_horse(dummy, first_horse, position, corde):

        dummy['hippo'] = first_horse['hippo'] # this fields depends of the race
        dummy['numcourse'] = first_horse['numcourse']
        dummy['cl'] = position
        dummy['dist'] = first_horse['dist']
        dummy['partant'] = first_horse['partant']
        dummy['typec'] = first_horse['typec']
        dummy['cheque'] = first_horse['cheque']
        dummy['numero'] = corde # to verify
        dummy['corde'] = corde
        dummy['createdat'] = first_horse['createdat']
        dummy['updatedat'] = first_horse['updatedat']

        return dummy


