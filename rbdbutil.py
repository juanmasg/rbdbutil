#!/usr/bin/env python3

import sys
import csv
import struct
import re
import hashlib
import argparse

as_int = lambda x: int.from_bytes(x, byteorder='little')
as_bytes = lambda x: int.to_bytes(x, byteorder='little', length=4)

def stdtrack(track):
    # Remove leading track numbers from track title
    if (re.match(r'^[0-9][0-9]?[- ]', track)):
        track = " ".join(track.split(' ')[1:])

    return track

def strstd(s):
    # remove characters irrelevant for matching
    s = s.replace('\'', "")
    s = s.replace("'", "")
    s = s.replace("&", "and")
    s = s.replace("?", "")
    s = s.replace(",", "")
    s = s.replace("-", "")
    s = s.replace("!", "")
    s = s.replace(".", "")
    s = s.replace("`", "")
    s = s.replace(" ", "")
    s = s.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    s = s.replace("Thirty", "30")
    if "(" in s:
        s = s.split('(')[0]

    return s.lower()

class Track:
    artist = None
    album = None
    title = None

    key = None

    def __init__(self, artist=None, album=None, title=None):
        self.artist = artist
        self.album = album
        self.title = title

        self.key = strstd(f"{artist}{album}{stdtrack(title)}")


    def eq(self, track):
        return self.key == track.key

    def eq_strict(self, track):
        return  self.artist == track.artist and \
                self.album == track.album and \
                self.title == track.title

    @staticmethod
    def from_rbdb_entry(entry):
        return Track(artist=entry.get("ARTIST"), album=entry.get("ALBUM"), title=entry.get("TITLE"))

    def __repr__(self):
        return f"{self.artist} - {self.album} - {self.title} -- {self.key}"


class RBDB:
    IDX = "idx"
    IDX_ARTIST = 0
    IDX_ALBUM = 1
    IDX_TITLE = 3
    IDX_FILE = 4
    IDX_COMPOSER = 5
    IDX_COMMENT = 6
    IDX_ALBUMARTIST = 7
    IDX_GROUPING = 8
    IDX_CANONICALARTIST = 12

    file_indexes = [
        IDX,
        IDX_ARTIST,
        IDX_ALBUM,
        IDX_TITLE,
        IDX_FILE,
        IDX_COMPOSER,
        IDX_COMMENT,
        IDX_ALBUMARTIST,
        IDX_GROUPING,
        IDX_CANONICALARTIST,
    ]

    paths = None
    datas = None
    checksums = None
    cur_offset = 0

    dbver = None
    dbsize = None
    dbcount = None
    dbserial = None
    dbcommit = None
    dbdirty = None

    positions = {
        "ARTIST" : 0,
        "ALBUM" : 1,
        "GENRE" : 2,
        "TITLE" : 3,
        "FILE" : 4,
        "COMPOSER" : 5,
        "COMMENT" : 6,
        "ALBUMARTIST" : 7,
        "GROUPING" : 8,
        "YEAR" : 9,
        "DISCNO" : 10,
        "TRACKNO" : 11,
        "CANONICALARTIST" : 12,
        "BITRATE" : 13,
        "LEN" : 14,
        "PLAYCOUNT" : 15,
        "RATING" : 16,
        "PLAYTIME" : 17,
        "LASTPLAYED" : 18,
        "COMMITID" : 19,
        "MTIME" : 20,
        "LASTELAPSED" : 21,
        "LASTOFFSET" : 22,
        "FLAGS" : 23,
    }

    ENTRY_LEN = 96


    def __init__(self, root):
        self.paths = {}
        self.datas = {}
        self.checksums = {}

        for i in self.file_indexes:
            self.paths[i] = f"{root}/database_{i}.tcd"
            self.datas[i] = bytearray(open(self.paths[i], "rb").read())
            self.checksums[i] = hashlib.md5(self.datas[i])

        self.dbver = as_int(self.datas[self.IDX][:4])
        self.dbsize = as_int(self.datas[self.IDX][4:8])
        self.dbcount = as_int(self.datas[self.IDX][8:12])
        self.dbserial = as_int(self.datas[self.IDX][12:16])
        self.dbcommit = as_int(self.datas[self.IDX][16:20])
        self.dbdirty = as_int(self.datas[self.IDX][20:24])

        self.cur_offset = 24


    def next_entry(self):
        entry = self.entry(self.cur_offset)
        self.cur_offset += self.ENTRY_LEN
        return entry

    def _get_string_at_offset(self, tagidx, offset):
        _len = as_int(self.datas[tagidx][offset:offset+4])
        _idx = as_int(self.datas[tagidx][offset+4:offset+8])
        _var = self.datas[tagidx][offset+8:offset+8+_len]
        s = []
        for x in _var:
            if x == 0:
                break
            s.append(x)

        return bytes(s).decode()

    def artist(self, offset):
        return _get_string_at_offset(self.IDX_ARTIST, offset)

    def entry(self, entry_offset):
        indexes = {}
        values = { "index": entry_offset }

        for pos_name, pos_offset in self.positions.items():
            offset = entry_offset + (pos_offset * 4)
            _idx = self.datas[self.IDX][offset:offset + 4]
            values[pos_name] = as_int(_idx)

            # value is mapped in a different file
            if pos_offset in self.file_indexes:
                file_idx = pos_offset
                values[pos_name] = self._get_string_at_offset(file_idx, values[pos_name])

        return values

    def EOF(self):
        return self.cur_offset >= len(self.datas[self.IDX])

    def update_numeric_field(self, index, field_name, newvalue):
        print(f"Update {field_name} for index {index}: {newvalue}")
        offset = index + (self.positions[field_name] * 4)
        newvalue_as_bytes = as_bytes(newvalue)
        for x in range(len(newvalue_as_bytes)):
            self.datas[self.IDX][offset + x] = newvalue_as_bytes[x]

    def get_numeric_field(self, index, field_name):
        offset = index + (self.positions[field_name] * 4)
        return as_int(self.datas[self.IDX][offset:offset+4])

    def update_entry(self, index, playcount=None, update_playtime=False):
        if playcount is not None:
            self.update_numeric_field(self.index, "PLAYCOUNT", playcount)

        if update_playtime:
            playcount = self.get_numeric_field(index, "PLAYCOUNT")
            _len = self.get_numeric_field(index, "LEN")
            new_playtime = playcount * _len
            self.update_numeric_field(index, "PLAYTIME", new_playtime)

    def commit(self):
        for i, d in self.datas.items():
            orig_sum = self.checksums[i]
            new_sum = hashlib.md5(d)
            if orig_sum.hexdigest() != new_sum.hexdigest():
                print(f"{self.paths[i]} changed {orig_sum.hexdigest()} != {new_sum.hexdigest()}")
                self.overwrite(i)

    def overwrite(self, index):
        with open(self.paths[index], "wb") as f:
            f.write(bytes(self.datas[index]))
        

class LastFM:
    data = None
    def __init__(self, csv_path):
        self.path = csv_path
        self._parse()

    def _parse(self):

        self.data = {}

        with open(self.path) as f:
            csvdata = f.read()
            if csvdata.startswith('\ufeff'):
                csvdata = csvdata[1:]
            csvdata_lines = csvdata.split("\n")
            row_count = len(csvdata_lines) - 1

            i = 0
            r = csv.DictReader(csvdata_lines, delimiter=';')
            for row in r:
                artist = row.get("Artist").strip().lower()
                album = row.get("Album").strip().lower()
                title = row.get("Track").strip().lower()
                i += 1
        
                track = Track(artist=artist, album=album, title=title)
                if track.key not in self.data:
                    self.data[track.key] = {
                        "count": 1,
                        "track": track,
                    }
                else:
                    self.data[track.key]["count"] += 1

    def count(self):
        return len(self.data)

    def artist_count(self):
        return len(set([ x["track"].artist.lower() for x in self.data.values()]))

    def album_count(self):
        return len(set([ x["track"].album.lower() for x in self.data.values()]))

    def exists(self, track):
        return self.data.get(track.key, None)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--dbdir", required=True)
    parser.add_argument("--lastfm")
    parser.add_argument("--info", "-I", action="store_true")
    parser.add_argument("--find", )
    parser.add_argument("--list-tracks", "-L", action="store_true")
    parser.add_argument("--import-counts", action="store_true")

    args = parser.parse_args()

    rbdb = RBDB(args.dbdir)

    print(f"File: {rbdb.paths[rbdb.IDX]}")
    print(f"Version: {rbdb.dbver}, Size: {rbdb.dbsize}, "
            f"Count: {rbdb.dbcount}, Serial: {rbdb.dbserial}, "
            f"Commit: {rbdb.dbcommit}, Dirty: {rbdb.dbdirty}")

    if args.info:
        sys.exit(0)

    lastfm = LastFM(args.lastfm) if args.lastfm else None
    if lastfm:
        print(f"LastFM tracks: {lastfm.count()}, artists: {lastfm.artist_count()},",
              f"albums: {lastfm.album_count()}")

    if args.list_tracks:
        while not rbdb.EOF():
            entry = rbdb.next_entry()
            print(f"{entry.get('ARTIST')} - {entry.get('YEAR')}"
                  f"- {entry.get('ALBUM')} - {entry.get('TRACKNO'):02d}"
                  f" - {entry.get('TITLE')} - {entry.get('LASTPLAYED')}, {entry.get('PLAYTIME')}")
        sys.exit(0)

    if args.find:
        while not rbdb.EOF():
            entry = rbdb.next_entry()
            track = Track.from_rbdb_entry(entry)
            if args.find.lower() in track.key:
                print(track)
        sys.exit(0)

    if args.import_counts:
        done = 0
        missing = 0
        while not rbdb.EOF():
            entry = rbdb.next_entry()
            track = Track.from_rbdb_entry(entry)
            if lastfm_entry := lastfm.exists(track):
                entry_playcount = entry.get("PLAYCOUNT")
                lastfm_playcount = lastfm_entry.get("count")

                # only update if lastfm has more plays.
                if lastfm_playcount > entry_playcount:
                    rbdb.update_entry(entry.get("index"), playcount=lastfm_entry.get("count"))

                if entry.get("PLAYCOUNT") > 0 and entry.get("PLAYTIME") == 0:
                    rbdb.update_entry(entry.get("index"), update_playtime=True)

                done += 1
            else:
                missing += 1

        print("Import: Done", ok, "Missing", ko)

        rbdb.commit()

