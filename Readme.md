## Confluent Producer and Consumer Example

This is a repository just for a sample code for how to produce and consume messages with a Confluent Account. If you dont have yet any Confluent account and want to play around with it, its free for research and testing purposes. Click here https://www.confluent.io/get-started/

The repository has many files.

utils.py its for read the client.properties files where resides all your credentials. You can copy and paste this client.properties from the confluent console.

cilent.properties is where you have configured your API KEY and Secret that was given in the conflent console.

main.py is the main file for produce messages. Utilizes the fake library for produce fake messages.

consumer.py is the consumer file for consume this messages.

Adapted from ANtonello Benedetto Code
https://towardsdatascience.com/how-to-build-a-simple-kafka-producer-and-consumer-with-python-a967769c4742

{
      "name": "user_name",
      "type": "string",
      "doc": "User name on string"
    },
    {
      "name": "user_address",
      "type": "string",
      "doc": "Address of user"
    },
    {
      "name": "platform",
      "type": "string",
      "doc": "Plaform that uses this"
    },
    {
      "name": "signup_at",
      "type": "string",
      "doc": "Date format"
    }