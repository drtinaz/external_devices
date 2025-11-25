For victron venus os devices running firmware version 3.60 or later.

*******************************************************************
This installs a service that will create virtual (external) devices for venus os that can be displayed on gui v2. (Relay modules, digital inputs, virtual batteries, temperature sensors, and tank sensors) This has been tested on Cerbo GX but most likely can also be used on Ekrano or raspi. Node red is not required, allowing this service to be installed on regular os versions. MQTT is the protocol used for status and control.

*******************************************************************
Installation is done via ssh
```
wget -O /tmp/download.sh https://raw.githubusercontent.com/drtinaz/external_devices/master/download.sh
bash /tmp/download.sh
```
