#!/usr/bin/env python
# coding: utf-8

# In[ ]:
import sys
from shutil import copyfile

import pika
import json
import time
import glob
import os
import hashlib  # to compute hash of the password
import subprocess
import shutil

##ROOT_MESSAGE_DIRECTORY = "/var/message"
ROOT_MESSAGE_DIRECTORY = "/Users/nishithasuvarna/message"
SEPARATOR = "----------------------------------------------"


def execute_shell_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True)
    output = process.stdout.readline()
    return_code = process.poll()
    if return_code == 0:
        return True
    return False


class HDFS:
    def check_if_directory_exists(self, directory):
        command = ['hdfs', 'dfs', '-test', '-d', directory]
        return execute_shell_command(command)

    def create_directory(self, directory):
        command = ['hdfs', 'dfs', '-mkdir', '-p', directory]
        return execute_shell_command(command)

    def store_file(self, filename, destination_directory):
        command = ['hdfs', 'dfs', '-put', filename, destination_directory]
        return execute_shell_command(command)

    def copy_message_directory_to_local_storage(self, source_directory, destination_directory):
        command = ['hdfs', 'dfs', '-get', source_directory, destination_directory]
        return execute_shell_command(command)


class MessageServer:

    def __init__(self):
        self.hdfs = HDFS()

    def start(self):
        # Set the connection parameters to connect to rabbit-server1 on port 5672
        # on the / virtual host using the username "rabbit" and password "1"
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
##        connection = pika.BlockingConnection(pika.ConnectionParameters('10.168.0.2', 5672, "/", pika.PlainCredentials('rabbit', '1')))
        self.channel = connection.channel()
        self.channel.queue_declare(queue=MAIL_QUEUE, durable=True)
        self.channel.queue_declare(queue=REQUEST_QUEUE, durable=True)
        #self.channel.queue_declare(queue=RESPONSE_QUEUE, durable=True)
        self.channel.queue_declare(queue=REGISTER_QUEUE, durable=True)

    def run(self):
        self.channel.basic_consume(queue=REGISTER_QUEUE, auto_ack=True, on_message_callback=self.register_callback)
        self.channel.basic_consume(queue=REQUEST_QUEUE, auto_ack=True, on_message_callback=self.request_callback)
        self.channel.basic_consume(queue=MAIL_QUEUE, auto_ack=True, on_message_callback=self.send_callback)
        self.channel.start_consuming()

    def create_sender_directory_for_receiver_if_not_exists(self, sender, receiver):
        sender_directory = ROOT_MESSAGE_DIRECTORY + "/" + receiver + "/received/" + sender + "/"
        if self.hdfs.check_if_directory_exists(sender_directory):
            return
        self.hdfs.create_directory(sender_directory)

    def store_message_in_receiver_box_in_hdfs(self, sender, receiver, filename):
        received_message_directory = ROOT_MESSAGE_DIRECTORY + "/" + receiver + "/received/" + sender + "/"
        self.hdfs.store_file(filename, received_message_directory)

    def create_sender_directory_for_sender_if_not_exists(self, sender):
        sender_directory = ROOT_MESSAGE_DIRECTORY + "/" + sender + "/sent/"
        if self.hdfs.check_if_directory_exists(sender_directory):
            return
        self.hdfs.create_directory(sender_directory)

    def store_message_in_sender_box_in_hdfs(self, sender, filename):
        sent_message_directory = ROOT_MESSAGE_DIRECTORY + "/" + sender + "/sent/"
        self.hdfs.store_file(filename, sent_message_directory)

    def retrieve_received_messages_from_hdfs(self, receiver, sender=""):
        if sender != "":
            message_directory = ROOT_MESSAGE_DIRECTORY + "/" + receiver + "/received/" + sender
            destination = "/var/tmp/" + receiver + "/received/" + sender
        else:
            message_directory =  ROOT_MESSAGE_DIRECTORY + "/" + receiver + "/received"
            destination = "/var/tmp/" + receiver

        if not os.path.isdir(destination):
            os.makedirs(destination, exist_ok=True)
        self.hdfs.copy_message_directory_to_local_storage(message_directory, destination)
        return destination + "/received"

    def retrieve_sent_messages_from_hdfs(self, sender):
        message_directory = ROOT_MESSAGE_DIRECTORY + "/" + sender + "/sent/"
        destination = "/var/tmp/" + sender + "/sent/"
        if not os.path.isdir(destination):
            os.path.mkdir(destination)
        self.hdfs.copy_message_directory_to_local_storage(message_directory, destination)
        return destination

    def construct_message_queue(self, message_directory):
        message_queue = ""
        file_list = glob.glob(message_directory + "/*/*")
        for file_entry in file_list:
            with open(file_entry, "rb") as file_handle:
                message_queue += str(file_handle.read()) + "\n" + SEPARATOR + "\n"
            os.remove(file_entry)
        return message_queue

    def register_callback(self, ch, method, properties, body):
        # TODO: directory creation and storing authentication info
        # user name and message
        try:
            register = json.loads(body)
            username = register.get('username')
            # computing hash of the password
##            password = hashlib.md5(register.get('password').encode('utf-8')).hexdigest()
            print("\nReceived registration request from user " + username)

            # inbox directory
            self.hdfs.create_directory(ROOT_MESSAGE_DIRECTORY + '/' + username + '/sent/')
            # outbox directory
            self.hdfs.create_directory(ROOT_MESSAGE_DIRECTORY + '/' + username + '/received/')

            print("\nSuccessfully registered the user " + username)

        except:
            print("Received invalid json file")
            print(sys.exec_info())

    def send_callback(self, ch, method, properties, body):

        """
        This method is called when a message is received by the message server
        commit update : All message is received from registered users which an inbox and an outbox
        """

        receivedMessage = json.loads(body)
        sender = receivedMessage.get('sender')
        receivers = receivedMessage.get('receipt')
        message = str(receivedMessage.get('message').encode("utf-8"))

        print("\nMessage arrived at the server. Here is the summary of the message:")
        print("\n[From]: " + sender)
        print("[Recipients]: " + receivers)
        print("[Message]: " + message)
        print("\n")

        ''' Create a new file '''
        filename = "/var/tmp/" + str(time.time()) + ".txt"
        file_handle = open(filename, 'w')
        file_handle.write("[From]: " + sender + "\n")
        file_handle.write("[Recipients]: " + str(receivers) + "\n")
        file_handle.write("[Time]: " + time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()) + "\n")
        file_handle.write("[Message]: " + message  + "\n")
        file_handle.close()

        self.create_sender_directory_for_receiver_if_not_exists(sender, receivers)
        self.store_message_in_receiver_box_in_hdfs(sender, receivers, filename)
        self.create_sender_directory_for_sender_if_not_exists(sender)
        self.store_message_in_sender_box_in_hdfs(sender, filename)
        print("\nSuccessfully stored the message history in the HDFS")
        os.remove(filename)

    def request_callback(self, ch, method, properties, body):
        """ Query types
            SENT: Get a list of all messages sent by the user
            RECEIPT: Get a list of all emails received by the user
        """

        request = json.loads(body)
        user = request.get('username')
        query_type = request.get('query_type')

        response_queue = request.get('response_queue')
        
        message_queue = ""
        if query_type == "SENT":
            print("\nReceived a request to fetch the outbox for the user: " + user)
            retrieved_directory = self.retrieve_sent_message_from_hdfs(user)
            message_queue = self.construct_message_queue(retrieved_directory)
        elif query_type == 'RECEIVED':
            print("\nReceived a request to fetch the inbox for the user: " + user)
            retrieved_directory = self.retrieve_received_messages_from_hdfs(user)
            message_queue = self.construct_message_queue(retrieved_directory)

        print("\nSending the requested message back to client. Message is as follows:\n")
        print(message_queue)
        self.channel.basic_publish(exchange='', routing_key=response_queue, body=message_queue)




if __name__ == "__main__":
    # TODO: create a persistent data for the mapping of sender and receiver to the file name

    if (len(sys.argv) <= 1):
        print("Please provide the server id as argument")

    SERVER_ID = str(sys.argv[1])
    MAIL_QUEUE = "MailQ" + SERVER_ID
    REGISTER_QUEUE = "RegisterQ" + SERVER_ID
    REQUEST_QUEUE = "RequestQ" + SERVER_ID
    RESPONSE_QUEUE = "ResponseQ"

    MESSAGE_SERVER = MessageServer()
    MESSAGE_SERVER.start()
    MESSAGE_SERVER.run()
