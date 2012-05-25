import zmq
import cPickle
from pythongrid import zloads

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://192.168.1.250:5000")

while True:
    msg_str = socket.recv()
    msg = zloads(msg_str)
    print "Got", msg
    socket.send("yehaa")


