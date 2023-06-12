# EventStore Kafka Streams Application

This project is an EventStore Kafka Streams Application that uses the Processor API. 

It is capable of taking input from multiple Kafka topics which match a particular regex pattern. 

The application processes JSON values from each topic, performs a differential operation, and then sends the output to their respective topics. The output contains "before", "after", and "diff" fields in JSON along with a timestamp. 

Additionally, the application can create output topics in Kafka if they are not already present.

The project is built using Maven.

## Prerequisites

- Java Development Kit (JDK)
- Apache Maven
- Apache Kafka

## Building the Project

To build the project, navigate to the root directory of the project in your terminal and execute the following command:

```bash
mvn clean package
```
This command compiles the source code, runs any tests, and packages the compiled code into a "fat" or "uber" JAR, including all necessary dependencies. The JAR file will be located in the target directory.

## Running the Application

Before running the application, you need to set some environment variables. These are:

- `EVENT_STORE_CDC_INPUT_TOPIC_PATTERN`: This defines the regex pattern that matches the input topics.
- `EVENT_STORE_CDC_DIFF_TOPIC_SUFFIX`: This defines the suffix for the differential topics.
- `EVENT_STORE_CDC_KEY_VALUE_STORE_SUFFIX`: This defines the suffix for the key-value store.
- `EVENT_STORE_CDC_JSON_DIFF_ENABLE`: This enables the differential operation on JSON.
- `EVENT_STORE_CDC_VALUE_JSON_PATH`: This enables the Path operation on JSON Value.

You can set these environment variables in the terminal like this:

```bash
export EVENT_STORE_CDC_INPUT_TOPIC_PATTERN=your_pattern
export EVENT_STORE_CDC_DIFF_TOPIC_SUFFIX=your_suffix
export EVENT_STORE_CDC_KEY_VALUE_STORE_SUFFIX=your_store_suffix
export EVENT_STORE_CDC_JSON_DIFF_ENABLE=1  # Enable with 1, disable with 0
export EVENT_STORE_CDC_VALUE_JSON_PATH=your_json_path
```
## Running the Application

To run the application, use the following command:

```bash
java -jar your-jar-name.jar
```

Replace your-jar-name.jar with the actual name of your JAR file.

Remember to replace the placeholders (your_pattern, your_suffix, your_store_suffix, your_json_path and your-jar-name.jar) with actual values where appropriate. 

## Topology 
![download](https://github.com/itsviveksinghania/EventStore_ProcessorAPI/assets/30683067/ce5bca4c-ca6a-4260-9be6-c5529bab3311)
