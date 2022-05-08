from __future__ import annotations

import argparse
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime
from time import sleep
from typing import List
from os.path import exists

import requests
import json


class BasicStationHandler(ABC):
    """
    The usage of an abstract class allows to define a series of functionalities.
    The one important thing to focus on when working with abstract classes is focusing on `what should happen` and
    forget about the `how it should happen`.
    This is the reason why methods in this class are abstract: only their signature is important (the what) while
    their implementation (the how) is not taken care of.
    An abstract class can not be instantiated.
    """

    @abstractmethod
    def save(self, stations: List[Station]) -> None:
        pass

    @abstractmethod
    def list(self) -> List[Station]:
        pass


class StationHandlerSQLite(BasicStationHandler):
    """
    This handler extends the abstract class :class:`BasicStationHandler <BasicStationHandler>`.
    It implements the abstract class BasicStationHandler by defining the code required for defining `how things
    should happen`.
    """

    def __init__(self, target_file: str = "stations.db") -> None:
        super().__init__()
        self._target_file = target_file

        conn = sqlite3.connect(self._target_file)
        cursor = conn.cursor()

        query = '''CREATE TABLE IF NOT EXISTS "station" (
            "id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            "name"	TEXT NOT NULL,
            "address"	TEXT NOT NULL,
            "station_id"	TEXT NOT NULL,
            "bikes"	INTEGER NOT NULL,
            "slots"	INTEGER NOT NULL,
            "total_slots"	INTEGER NOT NULL,
            "latitude"	NUMERIC NOT NULL,
            "longitude"	NUMERIC NOT NULL,
            "timestamp"	TEXT NOT NULL,
            "city"	TEXT NOT NULL
        )'''
        cursor.execute(query)
        conn.close()

    def save(self, stations: List[Station]) -> None:
        conn = sqlite3.connect(self._target_file)
        cursor = conn.cursor()
        # TODO adjust query
        query = """INSERT into station 
            (name, address, station_id, bikes, slots, total_slots, latitude, longitude, timestamp, city) 
            VALUES 
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        for station in stations:
            cursor.execute(query, (
                    station.name, station.address, station.station_id, station.bikes, station.slots,
                    station.total_slots, station.latitude, station.longitude, station.measurement_dt.isoformat(), station.city
                )
           )
        conn.commit()
        conn.close()

    def list(self) -> List[Station]:
        stations: List[Station] = []
        conn = sqlite3.connect(self._target_file)
        cursor = conn.cursor()

        query = "SELECT name, address, station_id, bikes, slots, total_slots, latitude, longitude, timestamp, city from station"
        cursor.execute(query)
        rows = cursor.fetchall()
        for name, address, station_id, bikes, slots, total_slots, latitude, longitude, timestamp , city in rows:
            stations.append(
                Station(
                    station_id, name, address, bikes, slots, total_slots, latitude,
                    longitude, datetime.fromisoformat(timestamp), city
                )
            )

        conn.close()
        return stations


class StationHandler(BasicStationHandler):
    """
    This handler extends the abstract class :class:`BasicStationHandler <BasicStationHandler>`.
    It implements the abstract class BasicStationHandler by defining the code required for defining `how things
    should happen`.
    """

    def __init__(self, target_file: str = "stations.json") -> None:
        super().__init__()
        self._target_file = target_file

        # Create database file if it does not exist
        if not exists(self._target_file):
            with open(self._target_file, "w") as f:
                json.dump([], f)

    def save(self, stations: List[Station]) -> None:
        with open(self._target_file, "r") as f:
            historical_stations = json.load(f)

            all_stations = historical_stations + [station.to_repr() for station in stations]

        with open(self._target_file, "w") as f:
            json.dump(all_stations, f, indent=4)

    def list(self) -> List[Station]:
        with open(self._target_file, "r") as f:
            raw_stations = json.load(f)
            return [Station.from_repr(raw_station) for raw_station in raw_stations]


class Station:

    def __init__(self, station_id: str, name: str, address: str, bikes: int, slots: int, total_slots: int, latitude: float, longitude: float, measurement_dt: datetime, city: str) -> None:
        super().__init__()
        self.station_id = station_id
        self.name = name
        self.address = address
        self.bikes = bikes
        self.slots = slots
        self.total_slots = total_slots
        self.latitude = latitude
        self.longitude = longitude
        self.measurement_dt = measurement_dt
        self.city = city

    def to_repr(self) -> dict:
        return {
            "id": self.station_id,
            "name": self.name,
            "address": self.address,
            "bikes": self.bikes,
            "slots": self.slots,
            "totalSlots": self.total_slots,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "timestamp": self.measurement_dt.isoformat(),
            "city": self.city,
        }

    @staticmethod
    def from_repr(raw_data: dict) -> Station:
        return Station(
            raw_data["id"],
            raw_data["name"],
            raw_data["address"],
            raw_data["bikes"],
            raw_data["slots"],
            raw_data["totalSlots"],
            raw_data["latitude"],
            raw_data["longitude"],
            datetime.fromisoformat(raw_data["timestamp"]),
            raw_data["city"]
        )


class StationBuilder:

    @staticmethod
    def from_trentino_data_hub_repr(raw_data: dict, collection_dt: datetime, city: str) -> Station:
        return Station(
            raw_data["id"],
            raw_data["name"],
            raw_data["address"],
            raw_data["bikes"],
            raw_data["slots"],
            raw_data["totalSlots"],
            raw_data["position"][0],
            raw_data["position"][1],
            collection_dt,
            city
        )


SOURCES_BY_CITY = {
    "trento": "https://os.smartcommunitylab.it/core.mobility/bikesharing/trento",
    "rovereto": "https://os.smartcommunitylab.it/core.mobility/bikesharing/rovereto",
}


if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser(description="This script is meant for supporting the manual boost of csv files.")
    arg_parser.add_argument("-s", "--sleep_interval", type=int, required=False, default=0, help="The sleep interval (expressed in seconds) between one collection and the successive one. Runs only once when set to 0.")
    arg_parser.add_argument("-j", "--json_file", type=str, required=False, default=None, help="The name of the JSON file where data should be stored. If specified a JSON file will be used for storage.")
    arg_parser.add_argument("--sqlite_db", type=str, required=False, default=None, help="The name of the SQLite database where data should be stored. If specified a SQLite database will be used for storage.")
    args = arg_parser.parse_args()

    if args.json_file:
        station_handler = StationHandler(target_file=args.json_file)
    elif args.sqlite_db:
        station_handler = StationHandlerSQLite(target_file=args.sqlite_db)
    else:
        print("Storage details are missing.")
        exit(1)

    while True:
        collection_datetime = datetime.now()
        new_stations = []

        for city, url in SOURCES_BY_CITY.items():
            print(f"Collecting updated data for city [{city}].")

            response = requests.request("GET", url)
            string_response = response.text
            bike_stations = json.loads(string_response)

            for raw_station in bike_stations:
                station = StationBuilder.from_trentino_data_hub_repr(raw_station, collection_datetime, city)
                new_stations.append(station)

        station_handler.save(new_stations)

        if args.sleep_interval > 0:
            sleep(args.sleep_interval)
        else:
            break
