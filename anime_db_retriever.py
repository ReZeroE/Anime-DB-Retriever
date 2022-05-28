import os
import sys
import json
import sqlite3
import time
from datetime import datetime

import AnilistPython
from AnilistPython import Anilist

class AniDatabaseRetriever:
    def __init__(self):
        self.MAX_ANIME_ID = 200000
        self.BULK_WRITE_THRESHOLD = 150
        self.RETRIEVER_VERSION = 'V2.0-SQLite3'
        self.RATELIMIT_OFFSET = 0.75
        self.ANILISTPYTHON_VERSION = "0.1.3"

        self.records = 0
        self.db_conn = sqlite3.connect("anime_database.db")
        self.anilist = Anilist()

    def create_database(self):
        cur = self.db_conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Anime_Records (
                id INTEGER,
                name_romaji TEXT,
                name_english TEXT,
                starting_time TEXT,
                ending_time TEXT,
                cover_image TEXT,
                banner_image TEXT,
                airing_format TEXT,
                airing_status TEXT,
                airing_episodes INTEGER,
                season TEXT,
                desc_para TEXT,
                average_score INTEGER,
                genres TEXT,
                next_airing_ep TEXT,

                x_record_updated_on DATETIME,
                x_retriever_version,

                PRIMARY KEY (id)
            );
        """)
        self.db_conn.commit()

    def bulk_insert(self, records: list):
        # self.db_conn.execute("DROP TABLE Anime_Records")

        curr_time = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        print(f"[{curr_time}] Writing {self.BULK_WRITE_THRESHOLD} records into DB...")
        try:
            self.db_conn.executemany("INSERT INTO Anime_Records VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", records)
        except sqlite3.ProgrammingError as ex:   # 1. incorrect number of fields supplied
            raise(ex)
        except sqlite3.IntegrityError as ex:     # 2. type mismatch / duplicate primary key
            raise(ex)
        except sqlite3.OperationalError as ex:   # 3. requested table doesn't exist
            print(f"Request table does not exist, creating table...")
            self.create_database()
            self.bulk_insert(records) # single recursive call for insert after table creation

        self.db_conn.commit()

    def initialize_values(self):
        curr_record_id = 0
        prev_record = self.db_conn.execute("SELECT * FROM Anime_Records ORDER BY id DESC LIMIT 1").fetchall()
        if len(prev_record) > 0: curr_record_id = prev_record[0][0] + 1
        return curr_record_id

    def retrieve_anime_data(self):
        curr_record_id = 0
        prev_record = self.db_conn.execute("SELECT * FROM Anime_Records ORDER BY id DESC LIMIT 1").fetchall()
        if len(prev_record) > 0: curr_record_id = prev_record[0][0] + 1

        anime_records = []
        while curr_record_id < self.MAX_ANIME_ID:

            # bulk writes to db for every 100 record retrieved
            if len(anime_records) == self.BULK_WRITE_THRESHOLD:
                self.bulk_insert(anime_records)
                anime_records = []

            try:
                anime_data = self.anilist.get_anime_with_id(curr_record_id)
                # print(anime_data)
                anime_tuple = (curr_record_id,)
                for label, content in anime_data.items():
                    if isinstance(content, list):
                        content = "|".join(content)
                    if label == "next_airing_ep" and content != None:
                        content = "|".join([str(time) for time in content.values()])
                    anime_tuple += (content,)
                anime_tuple += (datetime.now().strftime("%m-%d-%Y %H:%M:%S"),)
                anime_tuple += (self.RETRIEVER_VERSION,)
                anime_records.append(anime_tuple)

                anime_name = anime_data["name_romaji"]
                curr_time = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                print(f"[{curr_time}] <{len(anime_records)} records pending> | Retrieving anime ID: {curr_record_id}, Anime Name: {anime_name}")

            except BaseException as ex:
                # print(ex)
                curr_time = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                print(f"[{curr_time}] <{len(anime_records)} records pending> | Retrieving anime ID: {curr_record_id}... Anime with such ID does not exist...")

            curr_record_id += 1
            time.sleep(self.RATELIMIT_OFFSET)

    def convert_time(self, seconds):
        seconds = seconds % (24 * 3600)
        hour = seconds // 3600
        seconds %= 3600
        minutes = seconds // 60
        seconds %= 60
        
        return "%d hr, %02d min, %02d secs" % (hour, minutes, seconds)


if __name__ == "__main__":
    db_ret = AniDatabaseRetriever()

    start = time.time()

    curr_time = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    header = f"\n\n=============================== [{curr_time}] ==============================="
    print(header)
    print(f"""
    Retriever Version: {db_ret.RETRIEVER_VERSION}
    Internal Support: AnilistPython V{db_ret.ANILISTPYTHON_VERSION}
    Ratelimit Offset: {db_ret.RATELIMIT_OFFSET} secs
    SQL Bulk Writes Threshold: {db_ret.BULK_WRITE_THRESHOLD} records
    Estimated Time Consumption: {db_ret.convert_time(db_ret.MAX_ANIME_ID * db_ret.RATELIMIT_OFFSET - db_ret.initialize_values())} 
    """)
    l = ['='] * (len(header) - 2)
    print("".join(l) + "\n\n")
    
    time.sleep(5)
    db_ret.create_database()
    db_ret.retrieve_anime_data()

    print("All records have been successfully retrieved... Program Terminating...")
    print(f"Time Consumption: [{db_ret.convert_time(time.time() - start)}]")

