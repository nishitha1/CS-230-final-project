'''

Reference: https://medium.com/swlh/lets-write-a-chat-app-in-python-f6783a9ac170
'''

from socket import AF_INET
from socket import socket
from socket import SOCK_STREAM
from threading import Thread

import sys
import pika
import time
import json
import client_constants
from server_message import HDFS as hd
from server_message import MessageServer as ms

clients = {}
addresses = {}
host = 'localhost'
port = 8018 #5672
buffer_size = 1024
address = (host, port)
server = socket(AF_INET, SOCK_STREAM)
server.bind(address)
SEND_COUNT = 0
REQUEST_COUNTER = 0
register_count = 0

#TODO: ADD num_servers, call to register_user to unicast, multicast and broadcast function definition

# This function intercepts the incoming connections from the clients
def accept_incoming_connections(channel, num_servers):
    """Sets up handling for incoming clients."""
    while True:
        client, client_address = server.accept()
        print("%s:%s has connected." % client_address)
        client.send(bytes("Connected to the server", "utf8"))
        addresses[client] = client_address
        Thread(target=handle_client, args=(client,channel, num_servers)).start()


def handle_client(client, channel, num_servers):  # Takes client socket as argument.
    """Handles a single client connection."""
    message = client.recv(buffer_size).decode("utf8")
    split = message.split('#')
    name = split[0]
    dest = ""
    if len(split) > 1:
        dest =  split[1]

    welcome = '%s Connected!! Type #QQ# to exit.' % name
    client.send(bytes(welcome, "utf8"))
    msg = "%s has joined" % name
    nm = ""
    register_broadcast(num_servers, channel)
    broadcast(msg, num_servers, channel)
    clients[client] = name
    
    # send the previous messages to the connected client
    old_messages = get_all_message_for_dest(name)
    #{'From': "", "Recip": "","Messages:""}

    # If it received any record
    if len(old_messages) > 0 :
        from_ = old_messages['From']
        to_ = old_messages['Recip']
        msgs = old_messages['Messages']
        individual_msg = msgs.split(',')
        for m in individual_msg:
            unicast(m, to_, from_)
    
    while True:
        message = client.recv(buffer_size).decode("utf8")
        split = message.split('#')
        msg = split[0]
        dest = split[1]

        if msg != bytes("#QQ#", "utf8"):
            if dest == "broadcast" :
                broadcast(msg, num_servers, channel, name)
            elif ',' not in dest :
                unicast(msg, dest, num_servers, channel, name)
            else :
                multicast(msg, dest, num_servers, channel, name)

        else:
            client.send(bytes("#QQ#", "utf8"))
            client.close()
            del clients[client]
            broadcast(bytes("%s has left" % name, "utf8"), num_servers, channel)
            break

def response_callback(ch, method, properties, body):
    print("\nThe fetched messages is as follows: ")
    print(str(body.decode("utf-8")))
    return body.decode("utf-8")

# Retrieves all the messages that were sent to the user before.
# It reads all the destinationa/*/message.txt files and retrieves the messages.
def get_all_message_for_dest(destination, num_servers, process_id):
    ''' returns a dictionary of strings
    The format is like this {'source' : 'message'}
    for example if UserA (destination) had received messages
    from UserB, and UserC a sample dictionary will look like -
    {UserA : msg1, UserA : msg2, UserC : msg3}'''
    #pika.ConnectionParameters('10.168.0.2', 5672, "/", pika.PlainCredentials('rabbit', '1')))
    global REQUEST_COUNTER
    print("\nSending a request to the mail server to fetch the mailbox for user " + user)
    connection = pika.BlockingConnection(
    pika.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    request_queue = client_constants.REQUEST_QUEUE + str((REQUEST_COUNTER % num_servers) + 1)
    response_queue = client_constants.RESPONSE_QUEUE + str(process_id)
    request = {"username": user, "query_type": "RECEIVED", "response_queue": response_queue}
    channel.queue_declare(queue=response_queue, durable=True)
    channel.basic_publish(exchange='',
                          routing_key=request_queue,
                          body=json.dumps(request))
    REQUEST_COUNTER += 1
    channel.basic_consume(queue=response_queue,
                          auto_ack=True,
                          on_message_callback=response_callback)
    channel.start_consuming()
##    message_queue = ""
##    print("\nReceived request to fetch all the messages received for user: " + destination)
##    retrieved_directory = ms.retrieve_sent_message_from_hdfs(destination)
##    message_queue = ms.construct_message_queue(retrieved_directory)
##    print("\nSending the requested message back to client. Message is as follows:\n")
##    print(message_queue)
##    self.channel.basic_publish(exchange='', routing_key=response_queue, body=message_queue)
##    return message_queue


# Store message in the file system. It stores the file in following manner -
# inside destination/source folder it creates a text or ureuses existing text file.
# And stores the message in the txt file. For example,
# UserA/UserB/message.txt
def store_message_in_db(source, destination, message):
    ''' store the message in the database '''
    ''' Create a new file '''
    print("\nSuccessfully stored the message in the HDFS")
##        filename = "/var/tmp/" + str(time.time()) + ".eml"
##        file_handle = open(filename, 'w')
##        file_handle.write("[From]: " + source + "\n")
##        file_handle.write("[Recipients]: " + str(destination) + "\n")
##        file_handle.write("[Time]: " + time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()) + "\n")
##        file_handle.write("[Message]: " + message  + "\n")
##        file_handle.close()
##        ms.create_sender_directory_for_receiver_if_not_exists(sender, receivers)
##        ms.store_message_in_receiver_box_in_hdfs(sender, receivers, filename)
##        ms.store_message_in_sender_box_in_hdfs(sender, filename)
##        print("\nSuccessfully stored the email in the HDFS")
        #TODO : check
##        os.remove(filename)
##    return
## register
def register_user(username, ch, queue):
    login = {'username': str(username)}
    login_info = json.dumps(login)
    print("\nSending a registration request for the user " + username)
    ch.basic_publish(exchange='', routing_key=queue, body=login_info)
    
## convert message as json
def send_message(sender, receiver, message_body, channel, queue):
    message = {"sender": sender, "receipt": receiver, "message": message_body}
    jsonMessage = json.dumps(message)
    channel.basic_publish(exchange='', routing_key=queue, body=jsonMessage)
    print("[x] sent out a message")

def register_broadcast(num_servers, channel, source=""):
    file_handle = open("message_list", 'w')
    for sock in clients:
        file_handle.write(sock + '\n')
        rqueue = client_constants.REGISTER_QUEUE + str((register_count % num_servers) + 1)
        print("Register queue for user " + source + " is " + rqueue)
        register_user(source, channel, rqueue)
        register_count += 1
    file_handle.close()

# Function to broadcast a message to everyone
def broadcast(msg, num_servers, channel, source=""):
    #file_handle = open("message_list", 'w')
    for sock in clients:
        sock.send(bytes(source + " : " + msg, "utf8"))
        #file_handle.write(sock + '\n')
        #rqueue = client_constants.REGISTER_QUEUE + str((register_count % num_servers) + 1)
        #print("Reg queue for user " + source + " is " + rqueue)
        #register_user(source, channel, rqueue)
        #register_count += 1
        if len(source) > 0:
            global SEND_COUNT
            queue = client_constants.MAIL_QUEUE + str((SEND_COUNT % num_servers) + 1)
            print("\nSending mail (sender: " + source + " and receiver: " + clients[sock] + ") through queue " + queue)
            send_message(source, clients[sock], msg, channel, queue)
            # update count
            SEND_COUNT += 1
            store_message_in_db(source, clients[sock], msg)
    #file_handle.close()

# Function to send a message to a particular user
##def unicast(msg, dest, source="", num_servers, channel):  
def unicast(msg, dest, num_servers, channel, source=""):  
    for sock in clients:
        if clients[sock] == dest:
            sock.send(bytes(source + " : " + msg, "utf8"))
        if len(source) > 0:
            global SEND_COUNT
            #rqueue = client_constants.REGISTER_QUEUE + str((register_count % num_servers) + 1)
            #print("Reg queue for user " + source + " is " + rqueue)
            #register_user(source, channel, rqueue)
            #REGISTER_COUNT += 1
            queue = client_constants.MAIL_QUEUE + str((SEND_COUNT % num_servers) + 1)
            print("\nSending mail (sender: " + source + " and receiver: " + clients[sock] + ") through queue " + queue)
            send_message(source, clients[sock], msg, channel, queue)
            # update count
            SEND_COUNT += 1
            store_message_in_db(source, clients[sock], msg)

# Function to send a message to a set of users
def multicast(msg, dest, num_servers, channel, source=""):
    #TODO: ADD num_servers
    dests = dest.split(',')
    for sock in clients:
        print(clients[sock])
        if clients[sock] in dests:
            sock.send(bytes(source + " : " + msg, "utf8"))
            if len(source) > 0:
                global SEND_COUNT
                #rqueue = client_constants.REGISTER_QUEUE + str((register_count % num_servers) + 1)
                #print("Reg queue for user " + source + " is " + rqueue)
                #register_user(source, channel, rqueue)
                #REGISTER_COUNT += 1
                queue = client_constants.MAIL_QUEUE + str((SEND_COUNT % num_servers) + 1)
                print("\nSending mail (sender: " + source + " and receiver: " + clients[sock] + ") through queue " + queue)
                send_message(source, clients[sock], msg, channel, queue)
                # update count
                SEND_COUNT += 1
                store_message_in_db(source, clients[sock], msg)


if __name__ == "__main__":
    server.listen(10)
    num_servers = int(sys.argv[1])
    register_count = 0
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    print("Waiting for connection...")
    ACCEPT_THREAD = Thread(target=accept_incoming_connections, args=(channel, num_servers))
    ACCEPT_THREAD.start()  # Starts the infinite loop.
    ACCEPT_THREAD.join()
    server.close()
    
##    connection = pika.BlockingConnection(pika.ConnectionParameters('10.168.0.2', 5672, "/", pika.PlainCredentials('rabbit','1')))

##    thread_list = []
##    for sender in email_list:
##        print(sender)
##        t = Thread(target=mail_sending_client.start_sender, args=(email_list[0], email_list, channel, num_servers))
##        thread_list.append(t)
##
##    print(len(thread_list))
##    for thread in thread_list:
##        thread.start()
##
##    for thread in thread_list:
##        thread.join()


