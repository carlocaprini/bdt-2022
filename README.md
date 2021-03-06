# BdT2022 - Lab project

This repository tracks the work performed during the University of Trento, Big Data Technologies - 2022 Lab lessons.

## Objective

This objective leverages the public data provided by the [Trentino Open Data Hub](https://dati.trentino.it/). In particular, the project relies on the data describing the status pf the [Bike sharing stations](https://dati.trentino.it/dataset/stazioni-bike-sharing-emotion-trentino).

## Assignments

### Lessons #7 & #8

The following should be completed.

1. A third _station handler_ (let's call it `StationHandlerMySQL`) should be created in order to support the data management for a MySQL database.

This new handler should expose the same functionalities of the other existing handlers.
In order to make this possible, the new handler will need to implement the `BasicStationHandler`.

### Lesson #6

The following should be completed.

1. A second _station handler_ (let's call it `StationHandlerSQLite`) should be created in order to support the data management for a SQLite database.
    
    In order to make the handlers easily reusable and exchangeable, _abstract classes_ and _class inheritance_ could be taken advantage of.

### Lesson #5

The following should be completed.

1. Model all the Bike Sharing Station information into a dedicated class (~10 attributes)
2. Allow to periodically fetch the updated information about the bike stations’ status
    * The fetch interval should be configurable
3. Store the newly fetched information into a file (for the time being). A dedicated class for managing the storage operations should be used
    * Store a list of station snapshots
    * List of the existing station snapshots
    
   Using a class will make it possible to make the code reusable.
   Also, using a class will make it easy (if not transparent?) to change from a file storage to a database storage in the next assignments.
