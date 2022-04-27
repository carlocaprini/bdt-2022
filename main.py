from __future__ import annotations

import argparse
from datetime import datetime
from time import sleep
from typing import List
from os.path import exists

import requests
import json


class StationHandler:

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
        pass


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
    args = arg_parser.parse_args()

    collection_datetime = datetime.now()

    while True:
        new_stations = []

        for city, url in SOURCES_BY_CITY.items():
            print(f"Collecting updated data for city [{city}].")

            response = requests.request("GET", url)
            string_response = response.text
            bike_stations = json.loads(string_response)

            for raw_station in bike_stations:
                station = StationBuilder.from_trentino_data_hub_repr(raw_station, collection_datetime, city)
                new_stations.append(station)

        station_handler = StationHandler()
        station_handler.save(new_stations)

        if args.sleep_interval > 0:
            sleep(args.sleep_interval)
        else:
            break
