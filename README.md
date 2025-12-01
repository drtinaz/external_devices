For victron venus os devices running firmware version 3.60 or later.

*******************************************************************
This installs a service that will create virtual (external) devices for venus os that can be displayed on gui v2. (Relay modules, digital inputs, virtual batteries, temperature sensors, and tank sensors) This has been tested on Cerbo GX but most likely can also be used on Ekrano or raspi. Node red is not required, allowing this service to be installed on regular os versions. MQTT is the protocol used for status and control.

*************** INSTALL USING SSH ******************
```
wget -O /tmp/download.py https://raw.githubusercontent.com/drtinaz/scripts/main/download.py
python /tmp/download.py
```

to uninstall:
```
bash /data/apps/external_devices/uninstall.sh
```
