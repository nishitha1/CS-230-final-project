"""Script for Tkinter GUI chat client."""
from socket import AF_INET, socket, SOCK_STREAM
from threading import Thread
import tkinter


# This method receives message from the server
def receive():
    while True:
        try:
            msg = client_socket.recv(buffer_size).decode("utf8")
            msg_list.insert(tkinter.END, msg)
        except OSError:
            break


def send(event=None):  # event is passed by binders.
    """Handles sending of messages."""
    msg = my_msg.get()
    dest = destination.get()
    deliverable = ""

    if msg == "Type your username" :
        return
    if dest == "Type your destination" :
        dest = ""

    destination.set("Type your destination")
    my_msg.set("")
    deliverable = msg + "#" + dest
    print(deliverable)

    client_socket.send(bytes(deliverable, "utf8"))

    if msg == "#QQ#":
        client_socket.close()
        top.quit()


def on_closing(event=None):
    """This function is called when the window is closed."""
    my_msg.set("#QQ#")
    send()

top = tkinter.Tk()
top.title("ZotChat")

messages_frame = tkinter.Frame(top)

# Set up message text field
my_msg = tkinter.StringVar()  # For the messages to be sent.
my_msg.set("Type your username")

# Set up destination text field
destination = tkinter.StringVar()
destination.set("Type your destination")

scrollbar = tkinter.Scrollbar(messages_frame)  # To navigate through past messages.
# Following will contain the messages.
msg_list = tkinter.Listbox(messages_frame, height=20, width=55, yscrollcommand=scrollbar.set)
scrollbar.pack(side=tkinter.RIGHT, fill=tkinter.Y)
msg_list.pack(side=tkinter.LEFT, fill=tkinter.BOTH)
msg_list.pack()
messages_frame.pack()

entry_field = tkinter.Entry(top, textvariable=my_msg)
entry_field.bind("<Return>", send)
entry_field.pack()

entry_field2 = tkinter.Entry(top, textvariable=destination)
entry_field2.bind("<Return>", send)
entry_field2.pack()

send_button = tkinter.Button(top, text="Send", command=send)
send_button.pack()

top.protocol("WM_DELETE_WINDOW", on_closing)

buffer_size = 1024
server_address = ('localhost', 8018)

client_socket = socket(AF_INET, SOCK_STREAM)
client_socket.connect(server_address)

receive_thread = Thread(target=receive)
receive_thread.start()
tkinter.mainloop()  # Starts GUI execution.