# CS-230-final-project


Write the UI
Implement the Client 
Log in. User inserts username and password, and client sends the information to server. The server makes data lookup in Hadoop and sends confirmation to the client. If the server sends OK, the client logs in. 
After logging in Server sends the following information
List of friends
List of chat history
Client name
Client personal information
The client can select a friend and send messages to him/her.
The client can log out.


Implement the Server -- a list of APIS
Database handler
Retrieve data from HTFS
User information
Message information
Store data to HDFS
User information
Message information
Intercept client request
Log in request
Registration request
Send message request
Log out request
Delete message
Optional: friend request
Respond to the client
Routes the message to the intended client
Sends login confirmation
Sends homepage info -- personal information


Maruf:
Implement the UI
Implement the server communication APIs

Nishitha:
Set up HDFS file system. Table -- <USER> (UserID, UserName, User Picture), <Message> (senderID, ReceiverID, time stamp, content), 
Implement python function to look-up data in the file system.
Implement python function to store data in the file system.
