**Overview**


You’ll be setting up a system with:

**Producer Service:** Sends messages to RabbitMQ.
**Consumer Services:** Processes messages from RabbitMQ (for health check, insert, delete, and read operations).
**RabbitMQ:** Acts as the message broker.


**Prerequisites**

**Docker:** Ensure Docker is installed (Windows, Ubuntu, MacOS).


**RabbitMQ Docker Image:** Pull the RabbitMQ image.


**Python:** Install Python (recommended) and relevant libraries.


**Postman:** For testing requests (or use cURL).


**Database:** Choose a database (MongoDB).



**Instructions**


Create a Docker network manually that will host the RabbitMQ image. Start a RabbitMQ container on the network created. Access this network through its gateway IP address to connect to RabbitMQ from producer/consumers. Totally, 2 networks should exist, one holding the RabbitMQ container and the other holding all the remaining microservices. Hint: The docker-compose file which spins up the producer and consumers ensures that they lie on the network automatically.

For the producer service:

It is a RabbitMQ client that can construct queues/exchanges and transfer the necessary data to consumers. The exchange to deliver the messages to one of the 4 different queues(one for each consumer) based on the binding/routing key.

An HTTP Server (Flask for Python/Express for NodeJS) to listen to health_check requests so that it can distribute it to the respective consumer.

This server will be listening to requests which are GET type and the health_check message should be sent as an argument for the request
The health check message can be any string. It is to check if the rabbitmq connection is established.
An HTTP Server to listen to insert_record requests so that it can distribute it to the respective consumer which will insert record into the database

The HTTP server will be the same one created previously and will follow a new path, or if they want, they can start it on a different port. This must be followed for the servers upcoming as well.
This server will be listening to requests which are POST type and the request must contain the following fields : Name, SRN and Section.
An HTTP Server to listen to read_database requests so that it can distribute it to the respective consumer which will retrieve all the records of the database.

This server will be listening to requests which are of GET type.
An HTTP Server to listen to read_database requests so that it can distribute it to the respective consumer which will retrieve all the records of the database.

This server will be listening to requests which are of GET type.
An HTTP Server to listen to delete_record requests so that it can distribute it to the respective consumer which will delete the record based on the SRN sent

This server will be listening to requests which are GET type where the SRN is passed as an argument. Based on the SRN passed, the consumer must delete that particular record.
For the consumer_one (health_check):

RabbitMQ Client to listen for incoming requests on the “health_check” queue and process it.

This consumer must acknowledge that the health-check message has been listened to through the “health_check” queue. (Simple Ack)

For the consumer_two (insert_record):

RabbitMQ Client to listen for incoming requests on the “insert_record” queue and process it.

This consumer must insert the record into the database (DB is students choice. Could use SQL,MongoDB,etc). The data to be inserted will be listened to through the “insert_record” queue.

For the consumer_three (delete_record):

RabbitMQ Client to listen for incoming requests on the “delete_record” queue and process it.

This consumer must delete a record from the database based on the SRN which has been listened to through the “delete_record” queue.

For the consumer_four (read_database):

RabbitMQ Client to listen for incoming requests on the “read_database” queue and process it.
This consumer must retrieve all the records present in the database.
Dockerizing the application

Make sure to expose the necessary ports for communication when creating Dockerfiles for the producer and consumer programs.

A docker-compose file must be created that runs the producer, consumers and the database microservice container.


**Test microservices communicating**


To check if the microservices are communicating, try the below instructions:
Run the docker-compose file using the command “docker-compose up” and check if all the containers are created. The HTTP server must start running and the consumers must be able to wait for messages through RabbitMQ queues.

For consumer_one :

Send a GET request to the server with the appropriate path & argument
Requests can be sent via Postman or cURL
Check if the message has been transmitted via the queue. Example URL :- bash http://{IP-Address}/{health_check route} 
For consumer_two :

Send a POST request to the server with the appropriate path & data to be sent.
Check if the data has been inserted into the database
For consumer_three :

Send a GET database request to the server with the appropriate path & argument
Check if the record has been deleted in the DB
For consumer_four :

Send a GET request to the server with the appropriate path
Check the content of the DB
To end the process, use the command “docker-compose down”
