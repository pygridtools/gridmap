import zmq
import socket
from pythongrid import zdumps

context = zmq.Context()
zsocket = context.socket(zmq.REQ)
zsocket.connect("tcp://192.168.1.250:5002")

host_name = socket.gethostname()
ip_address = socket.gethostbyname(host_name)

msg_container = {}
msg_container["job_id"] = 0
msg_container["host_name"] = host_name
msg_container["ip_address"] = ip_address
msg_container["command"] = "test"
msg_container["data"] = None

msg_string = zdumps(msg_container)

zsocket.send(msg_string)

