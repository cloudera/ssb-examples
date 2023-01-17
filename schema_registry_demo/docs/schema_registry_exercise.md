# Setup (before the training)
To complete this demo you need a running Schema Registry service on a cluster. While using CSP-CE Schema Registry is available at http://localhost:7788/ui/#/.

# Schemas in Schema Registry

To start off you will need to create the following schemas in [Schema Registry](http://localhost:7788)

<details>
<summary>
Airport Weather
</summary>

- Name: `airport-weather`
- Description: `Temperature and visibility data at airports`
- Type: `Avro schema provider`
- Schema group: `Kafka`
- Compatibility: `Backward`
- Schema:

```json
{
 "namespace": "com.cloudera.ssb.training.schemaregistry.data",
 "type": "record",
 "name": "AirportWeather",
 "ssb.rowtimeAttribute": "eventTimestamp",
 "ssb.watermarkExpression": "`eventTimestamp` - INTERVAL '30' SECOND",
 "fields": [
  {
   "name": "airport",
   "type": "string"
  },
  {
   "name": "temperature",
   "type": "string"
  },
  {
   "name": "visibility",
   "type": "string"
  },
  {
   "name": "eventTimestamp",
   "type": [
    "null",
    {
     "type": "long",
     "logicalType": "timestamp-millis"
    }
   ]
  }
 ]
}
```

</details>

<details>
<summary>
Takeoffs
</summary>

- Name: `takeoffs`
- Description: `Data of plane takeoffs`
- Type: `JSON schema provider`
- Schema group: `Kafka`
- Schema:
```json
{
 "$schema": "http://json-schema.org/draft-07/schema#",
 "title": "TakeOffs",
 "type": "object",
 "additionalProperties": false,
 "$ssb.rowtimeAttribute": "eventTimestamp",
 "$ssb.watermarkExpression": "`eventTimestamp` - INTERVAL '15' SECOND",
 "properties": {
  "aircraft": {
   "type": "string"
  },
  "airport": {
   "type": "string"
  },
  "eventTimestamp": {
   "type": "string",
   "format": "date-time"
  }
 },
 "required": [
  "aircraft",
  "airport",
  "eventTimestamp"
 ]
}
```
</details>

<details>
<summary>
Weather Report
</summary>

- Name: `weather-report`
- Description: `weather-report`
- Type: `Avro schema provider`
- Schema group: `Kafka`
- Compatibility: `Backward`
- Schema:
```json
{
	"type": "record",
	"name": "WeatherReport",
	"namespace": "com.cloudera.ssb.training.data.avro",
	"fields": [
	{
	"name": "longitude",
	"type": {
		"type": "string",
		"avro.java.string": "String"
	}
	},
	{
	"name": "latitude",
	"type": {
		"type": "string",
		"avro.java.string": "String"
	}
	},
	{
	"name": "temperature",
	"type": {
		"type": "string",
		"avro.java.string": "String"
	}
	},
	{
	"name": "eventTimestamp",
	"type": [
		"null",
		{
		"type": "long",
		"logicalType": "timestamp-millis"
		}
	]
}
	]
}
```
</details>

<details>
<summary>
Converted Weather Report
</summary>

- Name: `weather-report-converted`
- Description: `Weather data at addresses and in Fahrenheit`
- Type: `JSON schema provider`
- Schema group: `Kafka`
- Schema:
```json
{
 "$schema": "http://json-schema.org/draft-07/schema#",
 "title": "ConvertedWeatherReport",
 "type": "object",
 "additionalProperties": false,
 "$ssb.rowtimeAttribute": "eventTimestamp",
 "$ssb.watermarkExpression": "`eventTimestamp` - INTERVAL '15' SECOND",
 "properties": {
  "address": {
   "type": "string"
  },
  "temperatureFahrenheit": {
   "type": "string"
  },
  "eventTimestamp": {
   "type": "string",
   "format": "date-time"
  }
 },
 "required": [
  "address",
  "temperature",
  "eventTimestamp"
 ]
}
```
</details>

# Exercise I
In this exercise there are two data streams, one containing the temperature (in Celsius) and visibility (in meters) information at airports at a time, and the other containing the time of takeoffs of an aircraft at an airport. The task is to join these to streams to get the weather data for each takeoff at the time of the takeoff.

## Create Kafka Data Provider in SSB
Explorer -> Data Sources -> Kafka -> New Kafka Data Source
- Name: `Kafka`
- Brokers: `kafka:9092`
- Protocol: `PLAINTEXT`

## Create Schema Registry Data Provider in SSB
Explorer -> Data Sources -> Catalog -> New Catalog
- Name: `schema-registry`
- Catalog Type: `Schema Registry`
- Kafka Cluster: `Kafka`
- Disable TLS
- Schema Registry URL: `http://schema-registry:7788/api/v1/`

## Generate fake `airport-weather` and `takeoffs` data

The first job of this demo project (`schema_as_sink` ) generates fake data which is inserted to the SR catalog tables.
```sql
DROP  TABLE  IF  EXISTS `airport-weather-faker`;
CREATE  TABLE `airport-weather-faker` WITH  (
'connector'  =  'faker',
'fields.airport.expression'  =  '#{Aviation.airport}',
'fields.temperature.expression'  =  '#{Weather.temperatureCelsius}',
'fields.visibility.expression'  =  '#{numerify ''###m''}',
'fields.eventTimestamp.expression'  =  '#{date.past ''30'',''SECONDS''}',
'rows-per-second'  =  '100'
)  LIKE `schema-registry`.`default_database`.`airport-weather` (EXCLUDING OPTIONS);

DROP  TABLE  IF  EXISTS `takeoff-faker`;
CREATE  TABLE `takeoff-faker` WITH  (
'fields.aircraft.expression'  =  '#{Aviation.aircraft}',
'fields.eventTimestamp.expression'  =  '#{date.past ''15'',''SECONDS''}',
'connector'  =  'faker',
'fields.airport.expression'  =  '#{Aviation.airport}',
'rows-per-second'  =  '100'
)  LIKE `schema-registry`.`default_database`.`takeoffs` (EXCLUDING OPTIONS);

INSERT  INTO `schema-registry`.`default_database`.`airport-weather`
	SELECT  *  FROM `airport-weather-faker`;
INSERT  INTO `schema-registry`.`default_database`.`takeoffs`
	SELECT  *  FROM `takeoff-faker`;
```

Do not stop the job yet.

## Insert `airport-weather` into keyed Kafka topic

Create the second job of this demo project (`schema_as_source`) (while keeping the first one running), which inserts the `airport-weather` stream into a keyed Kafka table, so it can be used in a temporal join.
```sql
DROP  TABLE  IF  EXISTS `airport-weather-kafka`;
CREATE  TABLE `airport-weather-kafka` (
`airport` VARCHAR(2147483647)  NOT  NULL,
`temperature` VARCHAR(2147483647)  NOT  NULL,
`visibility` VARCHAR(2147483647)  NOT  NULL,
`eventTimestamp` TIMESTAMP(3),
WATERMARK FOR `eventTimestamp` AS `eventTimestamp` - INTERVAL '30'  SECOND,
PRIMARY  KEY  (airport)  NOT ENFORCED
)  WITH  (
'connector'  =  'upsert-kafka',
'topic'  =  'airport-weather-kafka',
'properties.request.timeout.ms'  =  '120000',
'properties.bootstrap.servers'  =  'kafka:9092',
'properties.group.id'  =  'testGroup',
'properties.auto.offset.reset'  =  'earliest',
'key.format'  =  'raw',
'value.format'  =  'json'
);

INSERT  INTO `airport-weather-kafka`
	SELECT  *  FROM `schema-registry`.`default_database`.`airport-weather`;
```
Do not stop the job yet.


## Temporal join

The job `temporal_join` includes the solution of the task by joining each takeoff to the airport weather condition as of the time when the plane was taking off.
```sql
SELECT
	t.aircraft,
	t.airport,
	w.temperature,
	w.`visibility`,
	t.eventTimestamp
FROM `schema-registry`.`default_database`.`takeoffs` t
JOIN `airport-weather-kafka` FOR SYSTEM_TIME AS  OF t.eventTimestamp AS w
ON t.airport = w.airport;
```

> **Note:** In order for this query to display results the previous two jobs must be running in the background.

After examining the results, you can now stop all 3 jobs.
# Exercise II
In this exercise there is a data stream of coordinates and temperature (in Celsius) data at given times, and we would like to convert those coordinates to addresses and convert temperature to Fahrenheit then insert the converted stream to another Kafka topic with a JSON schema registered in the Schema Registry. 

## Create JavaScript UDF
To convert the temperature degrees from Celsius to Fahrenheit, we are creating a JavaScript UDF.
Explorer -> Functions -> New Function
- Name : `CELSIUSTOFAHRENHEIT`
- Description: `Converts a string formatted <Celsius temperature>°C to <Fahrenheit temperature>°F.`
- Output type: `STRING`
- Input type: `STRING`
- Function (JavaScript):
```javascript
function CELSIUSTOFAHRENHEIT(input){
   var celsius = parseInt(input.substring(0, input.length - 2));
   var fahrenheit = (celsius * 9 / 5 + 32).toFixed(2);
   return fahrenheit + '\xB0F';
}

CELSIUSTOFAHRENHEIT($p0);
```
- Test input: 16°C (output is 60.8°F)

## Upload Java UDF
To convert coordinates to address, we are using a Java UDF. Download the jar of [`lookup-address`](../resources/lookup-address/target/lookup-address-1.0-jar-with-dependencies.jar) UDF then upload it to SSB.
Explorer -> Functions -> Upload JAR

> **Note:** In case a jar of the same name is already uploaded, you get an error message saying `duplicate key value violates unique constraint`. You have two options to resolve this issue:
> - Rename the jar and upload it again.
> - Create the java UDF using the below statement (where `LOOKUP` is the name of the UDF you are creating and `LookupAddress` is the fully qualified class name of the Java class implementing the function)
> ```sql
> CREATE FUNCTION LOOKUPADDRESS AS 'LookupAddress' LANGUAGE JAVA
> ```

> **Note:** For further information on how to implement UDFs check out the [Flink Implementation Guide](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/functions/udfs/#implementation-guide).

## Generate fake weather-report data
Create a new job called `weather_report_generator`. Run the following query:

```sql
DROP  TABLE  IF  EXISTS `weather-report-faker`;
CREATE  TABLE `weather-report-faker` WITH (
'connector'  =  'faker',
'fields.longitude.expression'  =  '#{Address.longitude}',
'fields.latitude.expression'  =  '#{Address.latitude}',
'fields.temperature.expression'  =  '#{Weather.temperatureCelsius}',
'fields.eventTimestamp.expression'  =  '#{date.past ''30'',''SECONDS''}',
'rows-per-second'  =  '100'
)  LIKE `schema-registry`.`default_database`.`weather-report` (EXCLUDING OPTIONS);

INSERT INTO `schema-registry`.`default_database`.`weather-report` SELECT * FROM `weather-report-faker`;
```

## SQL Query
Create a new job called `udf`.
Insert the converted data into Schema Registry catalog table `weather-report-converted`.

```sql
INSERT INTO  `schema-registry`.`default_database`.`weather-report-converted`
    SELECT
        LOOKUPADDRESS(longitude, latitude) AS `address`,
        CELSIUSTOFAHRENHEIT(temperature) AS temp_fahrenheit,
        eventTimestamp
    FROM `schema-registry`.`default_database`.`weather-report`;
```

> **Note:** In order for this query to display results the weather_report_generator job must running in the background.

# Exercise

Extend the job `temporal_join` by converting Celsius degrees to Fahrenheit (temperature) and meters to feet (visibility) using JavaScript UDFs. Upload a Java UDF that returns the name of the pilot based on the airport, aircraft and takeoff time. Download the jar of [`get-pilot`](../resources/get-pilot/target/get-pilot-1.0-jar-with-dependencies.jar) UDF to create the Java UDF. The method `GETPILOT` takes three arguments: aircraft (String), airport (String), eventTimestamp (Timestamp).

<details> <summary> Solution </summary>

**Java UDF**

Upload the jar [`get-pilot`](../resources/get-pilot/target/get-pilot-1.0-jar-with-dependencies.jar) or if the jar is already uploaded, rename the jar or use the below script.
```sql
CREATE FUNCTION GETPILOT AS 'GetPilot' LANGUAGE JAVA
```

**JavaScript UDF**

Create a JavaScript UDF.

- Name : `METERTOFEET`
- Description: `Converts a string formatted <distance in meter>m to <distance in feet>ft.`
- Output type: `STRING`
- Input type: `STRING`
- Function (JavaScript):
```javascript
function METERTOFEET(input) { 
   var meter = parseInt(input.substring(0, input.length - 1));
   var feet = (meter * 3.28084).toFixed(2);
   return feet + 'ft';
}

METERTOFEET($p0);
```

**SQL Query**

Create a new job and execute the below query.

```sql
DROP TABLE IF EXISTS `takeoff-weather`;
CREATE TABLE  `takeoff-weather` (
  `pilot` VARCHAR(2147483647),
  `aircraft` VARCHAR(2147483647),
  `airport` VARCHAR(2147483647),
  `temp_fahrenheit` VARCHAR(2147483647),
  `visibility_feet` VARCHAR(2147483647),
  `eventTimestamp` TIMESTAMP(3)
) WITH (
  'properties.auto.offset.reset' = 'earliest',
  'properties.bootstrap.servers' = 'kafka:9092',
  'connector' = 'kafka',
  'properties.request.timeout.ms' = '120000',
  'value.format' = 'json',
  'topic' = 'takeoff-weather',
  'properties.group.id' = 'testGroup'
);


INSERT INTO `takeoff-weather`
    SELECT
        GETPILOT(t.aircraft, t.airport, t.eventTimestamp) AS `pilot`,
        t.aircraft,
        t.airport,
        CELSIUSTOFAHRENHEIT(w.temperature) AS `temp_fahrenheit`,
        METERTOFEET(w.`visibility`) AS `visibility_feet`,
        t.eventTimestamp
    FROM `schema-registry`.`default_database`.`takeoffs` t
    JOIN `airport-weather-kafka` FOR SYSTEM_TIME AS OF t.eventTimestamp AS w
    ON t.airport = w.airport;
```
	
> **Note:** In order for this query to display results the `schema_as_source` and `schema_as_sink` jobs must be running in the background.


</details>
