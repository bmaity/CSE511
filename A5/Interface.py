#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import thread
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    print("---Parallel Sort")
    maxrating = 5

    cur = openconnection.cursor()
    cmd = "SELECT MIN(%s) FROM %s" % (SortingColumnName, InputTable)
    cur.execute(cmd)
    min = cur.fetchone()[0]

    cmd = "SELECT MAX(%s) FROM %s" % (SortingColumnName, InputTable)
    cur.execute(cmd)
    max = cur.fetchone()[0]

    interval = abs(max - min) / maxrating

    cmd = "DROP TABLE IF EXISTS %s" % OutputTable
    cur.execute(cmd)
    cmd = "CREATE TABLE IF NOT EXISTS %s (LIKE %s)" % (OutputTable, InputTable)
    cur.execute(cmd)

    # loop through 0 to maxpartitions
    # sort values in the range_part table and insert into a new table output_i
    # when completed for all range partition tables then insert all the values of output_i tables into OutputTable

    for i in range(0, int(max)):
        cmd = "DROP TABLE IF EXISTS %s%s" % (OutputTable, str(i))
        cur.execute(cmd)
        cmd = "CREATE TABLE IF NOT EXISTS %s%s (LIKE %s)" % (OutputTable, str(i), InputTable)
        cur.execute(cmd)
        thread = threading.Thread(target=sortvalues(i, InputTable, SortingColumnName, OutputTable, openconnection))
        thread.start()

    for i in range(0, int(max)):
        cmd = "INSERT INTO %s SELECT * FROM %s%s" % (OutputTable, OutputTable, str(i))
        cur.execute(cmd)

    openconnection.commit()


def sortvalues(i, table, col, output, con):
    print("--sort %i" % i)
    cur = con.cursor()
    cmd = "INSERT INTO %s%s SELECT * FROM %s ORDER BY %s" % (output, str(i), table, col)
    cur.execute(cmd)
    cur.close()


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    pass # Remove this once you are done with implementation


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()
