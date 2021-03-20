## TODO Client Side
  * UI
  * Implement the Client 
  * Client sends the information to server. The server stores the information in hdfs and sends confirmation to the client.
  * After logging in server sends the following information
    * Friend status
    * Chat history
    * Client name
    * The client can select a friend and send messages to him/her.
    * The client can log out.

## TODO Server Side
  * Intercept client request
  * Registration request
  * Send message request
  * Database handler
  * Store data to HDFS
  * Retrieve data from HTFS
  * Delete message
  
## Optional: friend request
Respond to the client
Routes the message to the intended client
Sends login confirmation
Sends homepage info -- personal information

## Task Breakdown
Maruf:
Implement the UI
Implement the server communication APIs

Nishitha:
Set up HDFS file system, RabbitMq
Implement python function to look-up data in the file system.
Implement python function to store data in the file system.
Implement python function to retrieve data in the file system.

Amisha:
Implement a python function that intercepts client GET and POST request that was made through SocketIO.
Implement a python function that can send data back to the client through SocketIO (or any other protocol). 


# Materials:
  * https://medium.com/swlh/how-to-make-simple-web-chat-8e9778f992c9
  * https://codeburst.io/building-your-first-chat-application-using-flask-in-7-minutes-f98de4adfa5d
  * https://www.twilio.com/blog/multi-room-web-chat-flask-react-twilio-conversations
  * https://pusher.com/tutorials/chat-flask-vue-part-3
  * https://www.rabbitmq.com/tutorials/tutorial-one-python.html
  * https://www.rabbitmq.com/tutorials/tutorial-two-python.html

