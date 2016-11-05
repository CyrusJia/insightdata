#by Cyrus Jia (cjia@usc.edu)
#for Insight Data Engineering 2016

#CSV Format:
#['time', ' id1', ' id2', ' amount', ' message']
#['2016-11-02 09:45:44', ' 9560', ' 2481', ' 38.26', ' Hangover cure ']


import os
import sys
import csv

class antifraud(object):
    friend_map = dict() # dictionary of [user id, {friends' userids}]
    #friend_second_map = dict()

    def __init__(self):

        self.batch_payment_path = sys.argv[1]
        self.stream_payment_path = sys.argv[2]
        self.feature_one_path = sys.argv[3]
        self.feature_two_path = sys.argv[4]
        self.feature_three_path = sys.argv[5]

        if os.path.exists(self.feature_one_path):
            os.remove(self.feature_one_path)
        if os.path.exists(self.feature_two_path):
            os.remove(self.feature_two_path)
        if os.path.exists(self.feature_three_path):
            os.remove(self.feature_three_path)


        self.f1 = open(self.feature_one_path, 'w')
        self.f2 = open(self.feature_two_path, 'w')
        self.f3 = open(self.feature_three_path, 'w')

    # reads in batch data without analysis
    def parseBatchData(self):
        with open(self.batch_payment_path, 'rU') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')

            firstRow = True
            for row in reader:
                if firstRow==None and len(row) == 5:
                    user1 = "".join(row[1].split())
                    user2 = "".join(row[2].split())
                    self.createFriendship(user1, user2)

                firstRow = None

    # reads stream data with analysis for each transaction
    # updates friendships along the way
    def parseStreamData(self):
        with open(self.stream_payment_path, 'rU') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            firstLine = True
            for row in reader:
                if not firstLine and len(row) == 5:
                    user1 = "".join(row[1].split())
                    user2 = "".join(row[2].split())
                    self.feature1(user1, user2)
                    self.feature2(user1, user2)
                    self.feature3(user1, user2)
                    self.createFriendship(user1, user2)

                firstLine = None

    # Check if user 2 is a direct friend of user1
    # Runtime: O(1)
    def isFirstDegree(self, user1, user2):
        if self.friend_map.has_key(user1):
            if user2 in self.friend_map[user1]:
                return True
        return None

    # Check if user1 and user2 have mutual friends
    # i.e. user1's friends intersect user2's friends is not the null set
    # Let x = # user1's friends
    # Let y = # user2's friends
    # Runtime: O(min(x,y))
    def isSecondDegree(self, user1, user2):
        if self.isFirstDegree(user1, user2):
            return True
        elif self.friend_map.has_key(user1) and self.friend_map.has_key(user2):
            F1 = self.friend_map[user1]
            F2 = self.friend_map[user2]
            if len(F1.intersection(F2)) > 0:
                return True
            else:
                return None
        else:
            return None

    # Check if user1 and user2 have mutual second degree friends
    # i.e. user1's friends' friends intersect user2's friends' friends
    # is not the null set
    # Let x = # of user1's friends' friends
    # Let y = # of user2's friends' friends
    # Runtime = O(x + y + min(x,y))
    def isFourthDegree(self, user1, user2):
        if self.isSecondDegree(user1, user2):
            return True
        if self.friend_map.has_key(user1) and self.friend_map.has_key(user2):
            F_secondDeg_1 = set()
            F_secondDeg_2 = set()
            F_secondDeg_1.update(self.friend_map[user1])
            F_secondDeg_2.update(self.friend_map[user2])

            for j in self.friend_map[user1]:
                if self.friend_map.has_key(j):
                    F_secondDeg_1.update(self.friend_map[j])
            for j in self.friend_map[user2]:
                if self.friend_map.has_key(j):
                    F_secondDeg_2.update(self.friend_map[j])

            # Optimized set intersection: return upon
            # single intersected elemnt is found
            if len(F_secondDeg_1) > len(F_secondDeg_2):
                for j in F_secondDeg_2:
                    if j in F_secondDeg_1:
                        return True
            else:
                for j in F_secondDeg_1:
                    if j in F_secondDeg_2:
                        return True
            return None

        else:
            return None

    # Adds user2 to user1's friends list and vice versa
    def createFriendship(self, user1, user2):
        if not self.friend_map.has_key(user1):
            self.friend_map[user1] = set()
        if not self.friend_map.has_key(user2):
            self.friend_map[user2] = set()


        if not self.isFirstDegree(user1, user2):
            self.friend_map.get(user1).add(user2)
            self.friend_map.get(user2).add(user1)


    def feature1(self, user1, user2):
        if self.isFirstDegree(user1, user2):
            self.f1.write('trusted\n')
        else:
            self.f1.write('unverified\n')

    def feature2(self, user1, user2):
        if self.isSecondDegree(user1, user2):
            self.f2.write('trusted\n')
        else:
            self.f2.write('unverified\n')

    def feature3(self, user1, user2):
        if self.isFourthDegree(user1, user2):
            self.f3.write('trusted\n')
        else:
            self.f3.write('unverified\n')


    def closeFiles(self):
        self.f1.close()
        self.f2.close()
        self.f3.close()


t = antifraud()
t.parseBatchData()
t.parseStreamData()
t.closeFiles()
