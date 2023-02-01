-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Tables from CSV files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Circuits Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/raseelformula1dl/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Races Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date STRING,
  time STRING,
  url STRING
) USING csv OPTIONS (
  path "/mnt/raseelformula1dl/raw/races.csv",
  header true
)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Tables from JSON files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Constructors Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors(constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS (
  path "/mnt/raseelformula1dl/raw/constructors.json"
)

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Drivers Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers(driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING json
OPTIONS (
  path "/mnt/raseelformula1dl/raw/drivers.json"
)

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Results Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results(resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING
)
USING json
OPTIONS (
  path "/mnt/raseelformula1dl/raw/results.json"
)

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Pitstops Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(raceId INT,
driverId INT,
stop INT,
lap INT,
time STRING,
duration STRING,
milliseconds INT
)
USING json
OPTIONS (path "/mnt/raseelformula1dl/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Qualifying Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING
)
USING json
OPTIONS (path "/mnt/raseelformula1dl/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Lap times Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/raseelformula1dl/raw/lap_times")

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;