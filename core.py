# -*- coding: utf-8 -*-

import mysql.connector
import datetime
import csv

import helper


##############################################################################
##############################################################################
# TODO: self.available_bijections will be a dictionnary with column name
#       as keys
# TODO: leave numeric types without change, only change categorical types
#       to numbers.
"""
This file contains the necessary to connect to the database and create the
needed files to machine learning.
"""

# Obs : the string that the database returns are unicode, take that in account.
# UPDATE: In python3, we can drop the unicode problems
# COLUMNS has 129 elements
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
    This class should contains almost everything present in this file.
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
        self.cursor = mydb.cursor(dictionary=True)  # dictionary is the good
                                                    # way
        #######################################################################

        #######################################################################
        """
        UPDATE: this is the way is implemented for the moment
        This idea of making all races the same size is being tested.
        I'm not sure how to manage inconsistent size data.

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
        This part is the list of global information that need to be mapped to
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

        # this is to keep type of columns at the moment of the bijection
        print("Creating record of column types")
        self.column_type = {}
        course_0 = self.get_specific_course(self.courses[0])[0]
        print(type(course_0))
        for column in self.columns:
            self.column_type[column] = type(course_0[column])
        print("Done.\n")
        print("="*100)

        print("="*100)
        self.available_bijections = {}
        print("Creating bijections...")
        # self.hippo_bijection =
        self.create_bijection(self.hippo, "hippo")
        print("    hippodromes")
        # self.chevaux_bijection =
        self.create_bijection(self.chevaux, "cheval")
        print("    horses")
        # self.ecuries_bijection =
        self.create_bijection(self.ecuries, "ecurie")
        print("    ecuries")
        # self.jockeys_bijection =
        self.create_bijection(self.jockeys, "jockey")
        print("    jockeys")
        # self.trainers_bijection =
        self.create_bijection(self.trainers, "entraineur")
        print("    trainers")
        # self.owners_bijection =
        self.create_bijection(self.owners, "proprietaire")
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

    def available_bijections(self):
        return self.available_bijections.keys()

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

    def get_date_of_course_from_numcourse(self, ncourse):
        if self.is_valid_course(ncourse):
            res = self.get_specific_course(ncourse)[0]
            return res['jour']

    ############################################################################

    ###########################################################################
    # Group of helper functions.

    # For columns like 'cheval', 'typec'... this wrap all types in a list
    # and returns it.
    def make_list_arg(self, column):
        lis = []  # list to return

        # query definition and execution
        query = "SELECT {} FROM cachedate GROUP BY {}".format(column, column)
        res = self.pass_query(query)

        # appending results
        for r in res:
            lis.append(r[column])
        return lis

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
    def define_select_columns_query(self, cs, excluded=False,
                                    where_condition=None):
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
    @staticmethod
    def present_dict_answer(list_dd):
        res = ""
        for dd in list_dd:
            i = 0
            for key in dd.keys():
                value = dd[key]
                # we need to be careful with the encoding of strings...
                if isinstance(value, str): # rememvber to use pyhton3!!!!
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
            raise Exception("ERROR: INVALID COURSE!!")

    # use now the bijection to map everything to numbers
    def from_course_make_vector_couple(self, numcourse, hs_columns, gb_columns):

        course_list_dict = self.complete_course(
            ncourse=numcourse) # is this the correct method ?

        # this part of the information is global for all horses in the race
        # now we join each horse in a vector and another vector for the results

        # extends both rows with a last position, a target.
        # for the moment make a list to keep the positions
        target = []

        # this is the first row of the csv file, we will always return the
        # first row for every race, even if it is only needed only once,
        # maybe after optimize the memory used
        tail = self.make_tail_of_vector(hs_columns)
        first_row = gb_columns.copy()
        first_row.extend(tail)

        # target appended
        first_row.append("target")

        # this one is the one with the information about the horses
        second_row = []

        # we need to put the global information also in the second row
        first_horse = course_list_dict[0]
        for gbc in gb_columns:
            value = self.transform_value_by_column(gbc,
                                                   first_horse[gbc],)
            # print("inside gb columns in make vector,column is ", gbc,
            # "value is", first_horse[gbc], "result is", value)
            if value != -1:
                second_row.append(value)
            else:
                second_row.append(' ')

        for horse in course_list_dict:
            # append position of the horse to the vector target
            target.append(horse["cl"])
            for column in hs_columns:
                # the transformation to numeric values is done here
                value = self.\
                    transform_value_by_column(column, horse[column])
                # print("inside hs columns in make vector,column is ", column,
                # "value is", horse[column], "result is", value)
                if value != -1:
                    second_row.append(value)
                else:
                    second_row.append(' ')

        # target appended
        target = TurfConnect.target_position_to_truly_number_vector(target)
        target = tuple(target)

        # testing second line here
        second_row.append(target)

        return first_row, second_row

    # this function is under test
    def write_course_to_csv(self, numcourse, dir_address, file_name,
                            hs_columns, gb_columns):
        first_row, second_row = self.\
            from_course_make_vector_couple(numcourse, hs_columns, gb_columns)

        try:
            ff = open(dir_address + "/" + file_name + ".csv", 'w+',
                      encoding='utf-8')
            csv_writer = csv.writer(ff)

            csv_writer.writerow(first_row)
            csv_writer.writerow(second_row)
        except Exception as e:
            print("COULDN'T WRITE TO FILE : {}".format(e))

    # all method should implement a way to make a sibling file to record
    # information to identify each csv file with the information that it takes:
    # - first date -> last date
    # - type(s) of course
    # - attributes used in the csv file
    def write_course_to_csv_all_by_type(self, dir_address, typec,
                                        hs_columns, gb_columns,
                                        all_out=False,
                                        all_typec=False,
                                        id_use=False):

        if all_typec:
            print("Doing all race types")
        else:
            print("Doing race type: {}".format(typec))

        if not all_typec and not (typec in self.type_course):
            raise Exception("UNKNOWN TYPE OF COURSE: ABORTING")
        else:
            # I need the first row only once
            inner_gb_columns = gb_columns.copy()

            if all_out:
                inner_hs_columns = self.columns.copy()
                for i in inner_gb_columns:
                    inner_hs_columns.remove(i)
            else:
                inner_hs_columns = hs_columns.copy()
            first_row, second_row = self.\
                from_course_make_vector_couple(self.courses[0],
                                               inner_hs_columns,
                                               inner_gb_columns)
            rows = []

            counter = 0
            length = len(self.courses)

            # way to keep date of latest and first race
            first_date = datetime.date(2100, 1, 1)
            last_date = datetime.date(1900, 1, 1)

            instances = 0

            for course in self.courses:
                # test that the race is of the good type
                if self.get_type_of_course_from_numcourse(course) == typec \
                        or all_typec:

                    # to put in the descrp file
                    instances += 1

                    this_date = self.get_date_of_course_from_numcourse(course)
                    if this_date < first_date:
                        first_date = this_date
                    if this_date > last_date:
                        last_date = this_date

                    not_needed_row, row = self.\
                    from_course_make_vector_couple(course, inner_hs_columns,
                                                   inner_gb_columns)
                    rows.append(row)
                    remaining = length - counter
                    percent = (counter / length) * 100
                    percent = "{0:.3f}% done".format(percent)
                    s = ("{} races remaining" + " "*20 + percent).\
                        format(remaining)
                    TurfConnect.print_inline(s)
                    counter += 1
            print("\nDone.")
            try:

                dump_name = open(dir_address + "/" + "dump_counter", 'r')
                dump_counter = int(dump_name.readline().strip())
                dump_name.close()
                dump_name = open(dir_address + "/" + "dump_counter", 'w')
                dump_name.write(str(dump_counter+1))
                dump_name.close()

                dump_name = "dump-"
                if all_out:
                    dump_name += "all_out-"
                if all_typec:
                    dump_name += "all_typec-"
                else:
                    dump_name += ("typec-" + typec)
                dump_name += (str(dump_counter) + "-")


                ff = open(dir_address + "/" + dump_name + ".csv", 'w+',
                          encoding='utf-8')

                # this is the descriptor file
                fdescrip = open(dir_address + "/" + dump_name + "descrp", 'w+',
                                encoding='utf-8')

                first_date = str(first_date)
                last_date = str(last_date)
                file_date = str(datetime.datetime.now())

                fdescrip_str = (("- Number of horses by race: %d\n"
                                 "- Instaces: %d\n"
                                 "- Total attributes: %d\n"
                                 "- First date: %s\n"
                                 "- Last date: %s\n"
                                 "- Race type: %s\n"
                                 "- Attributes by horse: %d\n"
                                 "    - %s\n"
                                 "- Attributes by race: %d\n"
                                 "    - %s\n"
                                 "- File created: %s")
                 % (self.normal, instances, len(first_row), first_date,
                    last_date, typec,
                    len(hs_columns),
                    str(hs_columns), len(gb_columns), str(gb_columns),
                    file_date)
                )

                fdescrip.write(fdescrip_str)

                # csv-file write-to part
                csv_writer = csv.writer(ff)

                # we need to add id to csv files
                id = []
                if id_use:
                    id = ["id"]
                    id.extend(first_row)
                else:
                    id.extend(first_row)

                # csv_writer.writerow(first_row)
                csv_writer.writerow(id)
                id = 0
                for row in rows:
                    l = []
                    if id_use:
                        l = [id]
                        l.extend(row)
                    else:
                        l.extend(row)

                    csv_writer.writerow(l)

                    id += 1
                    # csv_writer.writerow(row)
            except Exception as e:
                print("COULDN'T WRITE TO FILE : {}".format(e))

    def update_to_latest_csv_file(self, dir_address, hs_columns,
                                  gb_columns):

        for typec in self.type_course:
            if len(typec) > 10:
                self.write_course_to_csv_all_by_type(dir_address, typec,
                                            hs_columns, gb_columns,
                                            all_out=False,
                                            all_typec=False,
                                            id_use=True)

        self.write_course_to_csv_all_by_type(dir_address, typec,
                                        hs_columns, gb_columns,
                                        all_out=False,
                                        all_typec=True,
                                        id_use=True)

        self.write_course_to_csv_all_by_type(dir_address, typec,
                                        hs_columns, gb_columns,
                                        all_out=True,
                                        all_typec=True,
                                        id_use=True)

    # this function create bijection between a list of something and numbers
    # usually we will us it to map not numeric values to numeric ones
    def create_bijection(self, li, column):
        if not (column in self.available_bijections.keys()):

            length = len(li)
            if length == 0:
                lis = self.make_list_arg(column)
            else:
                lis = li.copy()

            bij_dict = helper.TWDict()
            length = len(lis)
            for i in range(length):
                bij_dict[lis[i]] = i

            # print("bijection created for column", column, "with length", len(lis))

            def f2(value):
                return bij_dict.get(value)

            self.available_bijections[column] = f2

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

    def make_tail_of_vector(self, columns):
        tail = []

        for i in range(self.normal):
            for column in columns:
                tail.append(column + str(i))

        return tail

    @staticmethod
    def list_to_full_string(li):
        res_li = []

        for i in li:
            res_li.append(str(i))

        return res_li

    # this will take a list a transform to a vector, we don't need
    # to further transform to a vector, maybe only to string
    @staticmethod
    def target_position_to_truly_number_vector(li):
        lis = []
        for i in li:
            str_i = str(i)
            if (str_i[:2]).isdigit():
                lis.append(int(str_i[:2]))
            elif (str_i[:1]).isdigit():
                lis.append(int(str_i[:1]))
            else:
                # another solution
                # lis.append(-1)
                lis.append(41)
        return tuple(lis)

    # this function should only change non-numeric values
    def transform_value_by_column(self, column, value):
        """
        [
            "hippo", "cheval", "pere", "mere", "ecurie", "dernierJoc", "jockey",
            "dernierEnt", "entraineur", "dernierProp", "proprietaire",
        ]
        :param column: a columns that is in the bijections set
        :param inverse: the way the mapping goes
        :param value: value to e transformed
        :return: a value
        """
        if TurfConnect.is_numeric_type(self.column_type[column]):
            return value
        elif column in self.available_bijections.keys():
            res = self.available_bijections[column](value)
            # print("column already exists, column, value", column, value, res)
            return res
        else:
            self.create_bijection([], column)
            res = self.available_bijections[column](value)
            # print("column created, column, value", column, value, res)
            return res

    # maybe change this after
    @staticmethod
    def is_numeric_type(a):
        return a in [type(2), type(2.2)]

    # this is a wrapper for lazy people like me
    @staticmethod
    def print_inline(s):
        print("\r", s, end="", flush=True)
