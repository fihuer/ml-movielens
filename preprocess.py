# -*- coding:Utf-8 -*-
# ML-Movielens Package
prog_name = "ML-Movielens - Preprocess"
# version:
version = 1
# License: Creative Commons Attribution-ShareAlike 3.0 (CC BY-SA 3.0) 
# (http://creativecommons.org/licenses/by-sa/3.0/)

##############
### IMPORT ###
##############
import os, time, sys

###############
### GLOBALS ###
###############

###############
### CLASSES ###
###############

def parse_users(filename):
    "Parse user <-> (Age, Gender, Profession)"
    results = {}
    with open(filename, "r") as f:
        line = f.readline()
        while line:
            tmp = line.split("|")
            results[tmp[0]] = (tmp[1], tmp[2], tmp[3])
            line = f.readline()

    return results

def parse_movies(filename):
    "Pares movies <-> (Unknown, Action, Adventure, Animation, Children's, Comedy, Crime, Documentary, Drama, Fantasy, Film-Noir, Horror, Musical, Mystery, Romance, Sci-Fi, Thriller, War, Western)"
    results = {}
    with open(filename, "r") as f:
        line = f.readline()
        while line:
            tmp = line.replace("\n","").replace("\r","").split("|")
            genres = []
            for i in range(5, len(tmp)):
                genres += tmp[i]
            results[tmp[0]] = genres
            line = f.readline()
    return results

def preprocess(data, users, movies, output):
    "Preprocess a file to a Tab file"
    u = parse_users(users)
    m = parse_movies(movies)
    with open(data, "r") as in_, open(output, "w") as out_:
        # Write headers
        out_.write("Age\tGender\tProfession\t" +
                   "Unknown\tAction\tAdventure\tAnimation\tChildren\tComedy\tCrime\tDocumentary\tDrama\tFantasy\tFilmNoir\tHorror\tMusical\tMystery\tRomance\tSciFi\tThriller\tWar\tWestern\t" +
                   "Rating" +
                   "\n")
        # Write type
        out_.write("c\td\ts\t" + ("d\t"*19) + "c\n")
        out_.write("\t"*3 + "\t"*19 + "class\n")
        # Write data
        line = in_.readline()
        while line:
            user = u[line.split("\t")[0]]
            movie = m[line.split("\t")[1]]
            rating = line.split("\t")[2]
            s = "\t".join(user) + "\t"
            s += "\t".join(movie) + "\t"
            s += rating
            s += "\n"
            out_.write(s)
            line = in_.readline()
        

###################
### DEFINITIONS ###
###################

##################
###  __MAIN__  ###
##################

if __name__ == "__main__":
    print "> Welcome to " + str(prog_name) + " (r" + str(version) + ")"
    print "> Loading program ..."
    
    if len(sys.argv) < 5:
        print "Usage: " + sys.argv[0] + " <data_file> <users_file> <movies_file> <out_file>"
        sys.exit()
    print "> Working..."
    preprocess(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    
