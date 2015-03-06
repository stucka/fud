#!/home/mstucka/bin/python
import MySQLdb
import datetime
import urllib
import sys
import os
import creds
# Requests?
#import glob
#import zipfile
#import csv

## Note "import creds" refers to MySQL credentials to be entered in creds.py
## chmod creds.py 700 for more security on Unix machines.

## Looking for four basic files.

## Fudmeta: Holds a "lastdate" field on a single row. Last successful 
## completion. Can also give it a "Counties" field that's returned as a 
## string that becomes a list of Counties we want to search. 
## Alternately, do we want a county-by-county date field?

## Fudplace: Master table of establishments, addresses parts, 
## geolocation. Include fields for corrected addresses and place names, 
## and store originals. One record per establishment.

## Fudscore: Restaurant ID, inspection ID, date of inspection, score of 
## inspection, count of types of violations, count of actual violations. 
## One record per inspection.

## Fudvio: Restaurant ID, inspection ID, violation number, violation 
## description, occurence count. One record per violation.

initialcountylist=["Bibb", "Houston", "Peach"]

hostdir=creds.access['hostdir']
dbhost=creds.access['dbhost']
dbuser=creds.access['dbuser']
dbpassword=creds.access['dbpassword']
dbdatabase=creds.access['dbdatabase']

dbhost=MySQLdb.connect(host=dbhost, user=dbuser, passwd=dbpassword, db=dbdatabase)
db=dbhost.cursor()

def main():
    print "Starting program ..."
    db.execute('''show tables like 'fudmeta';''')
    dbreturn=db.fetchone()
    if not dbreturn:
        print "No database found. Starting a new one."
        RestartDatabaseFromScratch()
    else:
        print "Database found."
        print "I'll update voter history data from the last two years."
        FirstYearICareAbout = datetime.datetime.today().year - 1
        MyYears = range(FirstYearICareAbout, datetime.datetime.today().year + 1)
    DownloadHistory()
    UnzipHistory()
    ParseHistory()
    ImportHistory()
    return


def RestartDatabaseFromScratch():    
    print "Trying to start a new database ..."
#    try:
#        db.execute("""drop table if exists voterhist;""")
#        print "Rows affected in database drop: %d" % db.rowcount
#    except MySQLdb.Error, e:
#        print "Error occurred: %s " % e.args[0]
#        print e
#        
    db.execute("""drop table if exists fudmeta;""")
    db.execute("""create table fudmeta (County varchar(50), lastcheck date default "2010-01-01" );""")
    db.execute("""drop table if exists fudplace;""")
    db.execute("""create table fudplace (placeid varchar(20), rawplace varchar(50), rawaddress varchar(100), cleanplace varchar(50), cleanaddress varchar(100));""")
    db.execute("""drop table if exists fudscore;""")
    db.execute("""create table fudscore (placeid varchar(20), inspid varchar(20), inspdate date, inspscore int, inspgrade varchar(1), viotypes int, viocount int);""")
    db.execute("""drop table if exists fudvio;""")
    db.execute("""create table fudvio (placeid varchar(20), inspid varchar(20), vioid varchar(5), viodesc varchar(50), viocount int);""")
    print "Tables created."
    
    for county in initialcountylist:
        fullstring='insert into fudmeta values ("' + county + '")'
        print "    Adding " + county + " to the search."
        db.execute(fullstring)

    return

def DownloadHistory():
    print "Beginning to download history files ..."
#    urlprefix = "http://www.sos.georgia.gov/elections/voter_registration/VoterHistory/"
    urlprefix = "http://sos.ga.gov/Elections/VoterHistoryFiles/"
    for MyYear in MyYears:
        try:
            fullurl = urlprefix + str(MyYear) + ".zip"
            print "    Downloading data: " + fullurl
            fullfile = hostdir + "/VoterHistory" + str(MyYear) + ".zip"
            urllib.urlretrieve(fullurl, fullfile)
        except:
            print "Something went wrong with download."

    return

def UnzipHistory():
    print "Beginning to unzip history files ..."
    for MyYear in MyYears:
        print "    Unzipping data for " + str(MyYear)
        mysourcefile = hostdir + "/VoterHistory" + str(MyYear) + ".zip"
        if os.path.exists(mysourcefile):
            try:
                zip = zipfile.ZipFile(mysourcefile)
                for subfile in zip.namelist():
                        zip.extract(subfile, hostdir)
            except:
                print "Problems unzipping " + mysourcefile
                    
    return

def ParseHistory():
    print "Beginning to parse history files ..."
    bigfilename=hostdir + "/bighistory.txt"
    bigfilehandle = open(bigfilename, 'wb')
    big = csv.writer(bigfilehandle, delimiter = '\t' )
    for MyYear in MyYears:
        if MyYear >= 2013:
	    mysourcefile = hostdir + "/VOTER_HISTORY_" + str(MyYear) + ".TXT"
        else:
            mysourcefile = hostdir + "/Voter History " + str(MyYear) + ".txt"

        print "    Beginning to parse " + mysourcefile
#        print "    Beginning to parse " + str(MyYear) + " now ..."
        if os.path.exists(mysourcefile):
            source = open(mysourcefile, 'r')
            for line in source:
                countycode=line[0:3]
                registrationnumber=line[3:11]
#### HEY! Yes. Yes, 2013 uses the new filename but not the new date structure.
		if MyYear > 2013:
                    electiondate=line[11:15] + "-" + line[15:17] + "-" + line[17:19]
		else:
                    electiondate=line[15:19] + "-" + line[11:13] + "-" + line[13:15]

                electiontype=line[19:22]
                party=line[22]
                absentee=line[23]
                electionyear=line[15:19]
                big.writerow([countycode, registrationnumber,
                                         electiondate, electiontype, party,
                                         absentee, electionyear])

            source.close()
            print "    Deleting file " + mysourcefile
            os.remove(mysourcefile)   # Delete annual text file

    bigfilehandle.close()
    return


def ImportHistory():
    print "Beginning to import parsed voter history ..."
    db.execute("""set autocommit=0;""")
    db.execute("""alter table voterhist disable keys;""")
    for MyYear in MyYears:
        print "    Deleting database records, if any, for year " + str(MyYear)
        db.execute('delete from voterhist where ElectionYear=%s', str(MyYear))
    print "Beginning to import the big file."
    db.execute("""LOAD DATA LOCAL INFILE 'bighistory.txt' into table voterhist fields terminated by "\t";""")
    print "    Rows affected in database add: %d" % db.rowcount
#    print "    Committing to database. Should be quick."
    print "    Re-enabling keys and commiting. This could take a while." 
    db.execute("""commit;""")
    db.execute("""alter table voterhist enable keys;""")
    db.execute("""set autocommit=1;""")
    bigfilename=hostdir + "/bighistory.txt"
    print "    Deleting parsed history file " + bigfilename
    os.remove(bigfilename)   # Delete annual text file
    print "Wow. I think we might be done."
    return


if __name__ == '__main__':
    main()
