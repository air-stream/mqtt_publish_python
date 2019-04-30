import time
import paho.mqtt.client as mqtt
import ssl

# Define event callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.isConnected = True
        print("Connected OK")
        return

    print("Connected with result code [rc]: " + str(rc))

def on_disconnect(client, userdata, rc):
    print("Disconnected From Broker")
    client.connected_flag = False

def on_message(client, userData, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

def on_publish(client, userData, mid):
    print("Published: mid: {}".format(mid))

def on_subscribe(client, userData, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

def on_unsubscribe(client, userData, mid):
    print("Subscribed: " + str(mid))

def on_log(client, userData, level, buf):
    print(buf)

def on_fail(e):
    print("Fail")

def dump(obj):
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))

# Create a client instance
port = 12051
port_ssl = 22051
host = "m16.cloudmqtt.com"
clientId = "airstream"
username = ""
password = ""
topic_in = "in"
topic_out = "out"
ka = 10
qos = 0 # 2

# Get instance
client = mqtt.Client(client_id=clientId)

# Connection Flag
client.isConnected = False

# Use SSL
if True:
    port = port_ssl
    client.useSSL = True
    client.tls_set("./certs/addtrustexternalcaroot.crt", tls_version=ssl.PROTOCOL_TLSv1_2)

# Setting event clalbacks
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect
client.on_publish = on_publish
client.on_subscribe = on_subscribe
client.on_log = on_log

# Connect
client.username_pw_set(username, password)
client.connect(host, port)
client.loop_start()

while client.isConnected != True:    #Wait for connection
    time.sleep(0.1)

try:
    while True:
        # Publish a message
        client.publish(topic_out, "my message", qos)

        print("Waiting for the next message")
        time.sleep(10)
except KeyboardInterrupt:
    client.disconnect()
    client.loop_stop()
