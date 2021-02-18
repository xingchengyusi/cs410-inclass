#!/usr/bin/env python3
# this program loads Census ACS data using basic, slow INSERTs
# run it with -h to see the command line options

import time
import psycopg2
import psycopg2.extras
import argparse
import csv
from io import StringIO
from constants import DBname, DBuser, DBpwd, TableName, Datafile


CreateDB = False  # indicates whether the DB table should be (re)-created
Year = 2015
INDEX = True
LOGGED = ""
TEMP = ""
BATCH = False
COPY_FROM = False


def row2vals(row) -> str:
    # handle the null vals
    for key in row:
        if not row[key]:
            row[key] = 0
        row["County"] = row["County"].replace(
            "'", ""
        )  # eliminate quotes within literals

    ret = f"""
       {Year},                          -- Year
       {row['CensusTract']},            -- CensusTract
       '{row['State']}',                -- State
       '{row['County']}',               -- County
       {row['TotalPop']},               -- TotalPop
       {row['Men']},                    -- Men
       {row['Women']},                  -- Women
       {row['Hispanic']},               -- Hispanic
       {row['White']},                  -- White
       {row['Black']},                  -- Black
       {row['Native']},                 -- Native
       {row['Asian']},                  -- Asian
       {row['Pacific']},                -- Pacific
       {row['Citizen']},                -- Citizen
       {row['Income']},                 -- Income
       {row['IncomeErr']},              -- IncomeErr
       {row['IncomePerCap']},           -- IncomePerCap
       {row['IncomePerCapErr']},        -- IncomePerCapErr
       {row['Poverty']},                -- Poverty
       {row['ChildPoverty']},           -- ChildPoverty
       {row['Professional']},           -- Professional
       {row['Service']},                -- Service
       {row['Office']},                 -- Office
       {row['Construction']},           -- Construction
       {row['Production']},             -- Production
       {row['Drive']},                  -- Drive
       {row['Carpool']},                -- Carpool
       {row['Transit']},                -- Transit
       {row['Walk']},                   -- Walk
       {row['OtherTransp']},            -- OtherTransp
       {row['WorkAtHome']},             -- WorkAtHome
       {row['MeanCommute']},            -- MeanCommute
       {row['Employed']},               -- Employed
       {row['PrivateWork']},            -- PrivateWork
       {row['PublicWork']},             -- PublicWork
       {row['SelfEmployed']},           -- SelfEmployed
       {row['FamilyWork']},             -- FamilyWork
       {row['Unemployment']}            -- Unemployment
	"""
    return ret


def initialize():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--createtable", action="store_true")
    parser.add_argument(
        "-i",
        "--index",
        action="store_false",
        default=True,
        help="whether using index and constrains before insert.",
    )
    parser.add_argument(
        "-l",
        "--logged",
        action="store_false",
        default=True,
        help="whether using logged table",
    )
    parser.add_argument(
        "-t",
        "--temp",
        action="store_true",
        default=False,
        help="whether use temper table",
    )
    parser.add_argument(
        "-b",
        "--batch",
        action="store_true",
        default=False,
        help="whether use batch insert",
    )
    parser.add_argument(
        "-p",
        "--copy",
        action="store_true",
        default=False,
        help="whether use copy from methods.",
    )
    args = parser.parse_args()

    global CreateDB
    CreateDB = args.createtable

    global INDEX
    INDEX = args.index

    if not args.logged:
        global LOGGED
        LOGGED = "UNLOGGED"

    if args.temp:
        global TEMP
        TEMP = "TEMP"

    global BATCH
    BATCH = args.batch

    global COPY_FROM
    COPY_FROM = args.copy


# read the input data file into a list of row strings
# skip the header row
def readdata(fname) -> list:
    print(f"readdata: reading from File: {fname}")
    with open(fname, mode="r") as fil:
        dr = csv.DictReader(fil)
        headerRow = next(dr)
        print(f"Header: {headerRow}")

        rowlist = []
        for row in dr:
            rowlist.append(row)

    return rowlist


# convert list of data rows into list of SQL 'INSERT INTO ...' commands
def getSQLcmnds(rowlist) -> list:
    cmdlist = []
    for row in rowlist:
        valstr = row2vals(row)
        cmd = f"INSERT INTO {TableName} VALUES ({valstr});"
        cmdlist.append(cmd)
    return cmdlist


# connect to the database
def dbconnect():
    connection = psycopg2.connect(
        host="localhost", database=DBname, user=DBuser, password=DBpwd
    )
    connection.autocommit = True
    return connection


# create the target table
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):
    if LOGGED == "UNLOGGED":
        print("use unlogged table")
    if TEMP == "TEMP":
        print("use temperature table")

    with conn.cursor() as cursor:
        cursor.execute(
            f"""
        	DROP TABLE IF EXISTS {TableName};
        	CREATE {TEMP} {LOGGED} TABLE {TableName} (
            	Year                INTEGER,
              CensusTract         NUMERIC,
            	State               TEXT,
            	County              TEXT,
            	TotalPop            INTEGER,
            	Men                 INTEGER,
            	Women               INTEGER,
            	Hispanic            DECIMAL,
            	White               DECIMAL,
            	Black               DECIMAL,
            	Native              DECIMAL,
            	Asian               DECIMAL,
            	Pacific             DECIMAL,
            	Citizen             DECIMAL,
            	Income              DECIMAL,
            	IncomeErr           DECIMAL,
            	IncomePerCap        DECIMAL,
            	IncomePerCapErr     DECIMAL,
            	Poverty             DECIMAL,
            	ChildPoverty        DECIMAL,
            	Professional        DECIMAL,
            	Service             DECIMAL,
            	Office              DECIMAL,
            	Construction        DECIMAL,
            	Production          DECIMAL,
            	Drive               DECIMAL,
            	Carpool             DECIMAL,
            	Transit             DECIMAL,
            	Walk                DECIMAL,
            	OtherTransp         DECIMAL,
            	WorkAtHome          DECIMAL,
            	MeanCommute         DECIMAL,
            	Employed            INTEGER,
            	PrivateWork         DECIMAL,
            	PublicWork          DECIMAL,
            	SelfEmployed        DECIMAL,
            	FamilyWork          DECIMAL,
            	Unemployment        DECIMAL
         	);	
    	"""
        )
        if INDEX:
            createIndex(cursor)

    print(f"Created {TableName}")


def createIndex(cursor) -> None:
    cursor.execute(
        f"""
        ALTER TABLE {TableName} ADD PRIMARY KEY (Year, CensusTract);
        CREATE INDEX idx_{TableName}_State ON {TableName}(State);
    """
    )


def clean_csv_value(value) -> str:
    if value is None:
        return r"\N"
    return str(value).replace("\n", "\\n")


def load(conn, rlist) -> None:
    with conn.cursor() as cursor:
        print(f"Loading {len(rlist)} rows")

        if COPY_FROM:
            print("copy from method")
            file_like_io = StringIO()
            for r in rlist:
                file_like_io.write(row2vals(r) + '\n')

            start = time.perf_counter()
            cursor.copy_from(file_like_io, TableName, sep=",")
            elapsed = time.perf_counter() - start
        elif BATCH:
            print("batch insert")
            start = time.perf_counter()
            psycopg2.extras.execute_batch(
                cursor,
                f"""
                        INSERT INTO {TableName} VALUES (
                        {Year},
                        %(CensusTract)s,
                        %(State)s,
                        %(County)s,
                        %(TotalPop)s,
                        %(Men)s,
                        %(Women)s,
                        %(Hispanic)s,
                        %(White)s,
                        %(Black)s,
                        %(Native)s,
                        %(Asian)s,
                        %(Pacific)s,
                        %(Citizen)s,
                        %(Income)s,
                        %(IncomeErr)s,
                        %(IncomePerCap)s,
                        %(IncomePerCapErr)s,
                        %(Poverty)s,
                        %(ChildPoverty)s,
                        %(Professional)s,
                        %(Service)s,
                        %(Office)s,
                        %(Construction)s,
                        %(Production)s,
                        %(Drive)s,
                        %(Carpool)s,
                        %(Transit)s,
                        %(Walk)s,
                        %(OtherTransp)s,
                        %(WorkAtHome)s,
                        %(MeanCommute)s,
                        %(Employed)s,
                        %(PrivateWork)s,
                        %(PublicWork)s,
                        %(SelfEmployed)s,
                        %(FamilyWork)s,
                        %(Unemployment)s
                        );
                        """,
                rlist,
            )
            elapsed = time.perf_counter() - start
        else:
            cmdlist = getSQLcmnds(rlist)
            print("normal insert method")
            start = time.perf_counter()
            for cmd in cmdlist:
                cursor.execute(cmd)

            # copy from temp table.
            if TEMP:
                cursor.execute(f"""
                CREATE TABLE {TableName} AS TABLE {TableName};
                """
                )
            elapsed = time.perf_counter() - start

        print(f"Finished Loading. Elapsed Time: {elapsed:0.4} seconds")


def main():
    initialize()
    conn = dbconnect()
    if CreateDB:
        createTable(conn)

    rlist = readdata(Datafile)

    load(conn, rlist)

    # if not alter index and primary keys, now do it.
    if not INDEX:
        with conn.cursor() as cursor:
            createIndex(cursor)
            print("create index after insert")


if __name__ == "__main__":
    main()
