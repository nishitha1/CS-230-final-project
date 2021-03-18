'''

Reference: https://medium.com/swlh/lets-write-a-chat-app-in-python-f6783a9ac170
'''

from socket import AF_INET
from socket import socket
from socket import SOCK_STREAM
from threading import Thread


clients = {}
addresses = {}
host = 'localhost'
port = 8009
buffer_size = 1024
address = (host, port)
server = socket(AF_INET, SOCK_STREAM)
server.bind(address)


def accept_incoming_connections():
    """Sets up handling for incoming clients."""
    while True:
        client, client_address = server.accept()
        print("%s:%s has connected." % client_address)
        client.send(bytes("Connected to the server", "utf8"))
        addresses[client] = client_address
        Thread(target=handle_client, args=(client,)).start()


def handle_client(client):  # Takes client socket as argument.
    """Handles a single client connection."""
    message = client.recv(buffer_size).decode("ascii")
    split = message.split('#')
    name = split[0]
    dest = split[1]

    print(name)
    welcome = '%s Connected!! Type #QQ# to exit.' % name
    client.send(bytes(welcome, "utf8"))
    msg = "%s has joined" % name
    broadcast(bytes(msg, "utf8"))
    clients[client] = name
    while True:
        message = client.recv(buffer_size)
        split = message.split('#')
        msg = split[0]
        dest = split[1]

        if msg != bytes("#QQ#", "utf8"):
            if dest == "broadcast" :
                broadcast(msg, name)
            elif ',' not in dest :
                unicast(msg, dest, name)
            else :
                multicast(msg, dest, name)

        else:
            client.send(bytes("#QQ#", "utf8"))
            client.close()
            del clients[client]
            broadcast(bytes("%s has left" % name, "utf8"))
            break

# Retrieves all the messages that were sent to the user before
def get_all_message_for_dest(destination):
    ''' returns a list of strings '''
    return []


# Store message in the mongodb. 
def store_message_in_db(source, destination, message):
    ''' store the message in the database '''
    return

# Function to broadcast a message to everyone
def broadcast(msg, source=""):  
    for sock in clients:
        sock.send(bytes(source + " : ", "utf8")+msg)
        if len(source) > 0:
            store_message_in_db(source, clients[sock], msg)

# Function to send a message to a particular user
def unicast(msg, dest, source=""):  
    for sock in clients:
        if clients[sock] == dest:
            sock.send(bytes(source + " : ", "utf8")+msg)
        if len(source) > 0:
            store_message_in_db(source, clients[sock], msg)

# Function to send a message to a set of users
def multicast(msg, dest, source=""):  
    dests = dest.split('#')
    for sock in clients:
        if clients[sock] in dests:
            sock.send(bytes(source + " : ", "utf8")+msg)
            if len(source) > 0:
                store_message_in_db(source, clients[sock], msg)


if __name__ == "__main__":
    server.listen(10)  
    print("Waiting for connection...")
    ACCEPT_THREAD = Thread(target=accept_incoming_connections)
    ACCEPT_THREAD.start()  # Starts the infinite loop.
    ACCEPT_THREAD.join()
    server.close()

