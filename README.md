# sprinkler
For controlling my house sprinklers with a Raspberry Pi and a custom PCB

mqttclient.service handles the MQTT listening for commands and sending responses
sprinklerer.service handles the GPIO pin changing to control different relays
scheduler.service handles checking for weather and sunrise/sunset (solar) data

sprinklerer interacts with other modules purely through redis/celery
I needed an abstraction layer that has exclusive control over the GPIOs to prevent all manner of negative effects with the sprinklers including:
    - More than one zone turning on at a time
    - Turning more than one zone off at a time (water hammer possible)
    - Thinking I turned all the relays/zones off, but then some dormant zombie process coming back and turning one/more back on
    - Some other process crashing and leaving the zones turned on forever

scheduler handles 

mqttclient is the only other module that interacts via redis/celery, just adding and cancelling sprinkling and stopping tasks

scheduler handles 