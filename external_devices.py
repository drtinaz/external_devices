#!/usr/bin/env python3

from gi.repository import GLib
import logging
import sys
import os
import random
import configparser
import time
import paho.mqtt.client as mqtt
import json
import re
import dbus
import dbus.bus
import traceback

# Add velib_python path
VELIB_PATH = "/opt/victronenergy/dbus-systemcalc-py/ext/velib_python"
if os.path.exists(VELIB_PATH):
    sys.path.insert(1, VELIB_PATH)
else:
    print(f"ERROR: velib_python path not found: {VELIB_PATH}")
    sys.exit(1)

# Import from velib_python
try:
    from vedbus import VeDbusService
    from ve_utils import unwrap_dbus_value
except ImportError as e:
    print(f"Failed to import from velib_python: {e}")
    sys.exit(1)

logger = logging.getLogger()

for handler in logger.handlers[:]:
    logger.removeHandler(handler)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

CONFIG_FILE_PATH = '/data/apps/external_devices/config.ini'


def get_vrm_instance(unique_id, service_type, preferred_instance=0):
    """
    Get a VRM instance number from com.victronenergy.settings.
    Based on the actual D-Bus API introspection.
    """
    try:
        bus = dbus.SystemBus()
        
        # Wait for settings service to be available
        timeout = 30
        count = 0
        while True:
            if 'com.victronenergy.settings' in bus.list_names():
                logger.info("com.victronenergy.settings service is available")
                break
            if count >= timeout:
                raise Exception("Timeout waiting for com.victronenergy.settings service")
            count += 1
            logger.info(f'Waiting for settings service ({count}/{timeout})')
            time.sleep(1)
        
        # The object path for this specific device's setting
        device_node_path = f'/Settings/Devices/{unique_id}'
        setting_path = f'{device_node_path}/ClassAndVrmInstance'
        default_value = f'{service_type}:{preferred_instance}'
        
        # First, check if the device node exists
        try:
            # Try to get the ClassAndVrmInstance BusItem object
            item_obj = bus.get_object('com.victronenergy.settings', setting_path)
            item_iface = dbus.Interface(item_obj, 'com.victronenergy.BusItem')
            value = item_iface.GetValue()
            # unwrap the value (it comes as a D-Bus variant)
            value = unwrap_dbus_value(value)
            logger.info(f"Found existing setting: {setting_path} = {value}")
            instance = int(value.split(':')[1])
            logger.info(f"Using existing VRM instance {instance} for {service_type} device {unique_id}")
            return instance
        except dbus.DBusException as e:
            logger.info(f"Setting {setting_path} not found, creating device node: {e}")
            
            # Create the device node and setting using AddSetting
            settings_obj = bus.get_object('com.victronenergy.settings', '/Settings/Devices')
            settings_iface = dbus.Interface(settings_obj, 'com.victronenergy.Settings')
            
            # For creating a device node, we need to create the ClassAndVrmInstance setting
            name = f'{unique_id}/ClassAndVrmInstance'
            
            # AddSetting returns an integer status (0 = success)
            result = settings_iface.AddSetting('', name, default_value, 's', 0, 0)
            if result != 0:
                raise Exception(f"AddSetting returned error code {result}")
            
            logger.info(f"Created device node and setting {setting_path} = {default_value}")
            
            # Wait a moment for the setting to be fully created
            time.sleep(0.2)
            
            # Read back the value
            item_obj = bus.get_object('com.victronenergy.settings', setting_path)
            item_iface = dbus.Interface(item_obj, 'com.victronenergy.BusItem')
            value = unwrap_dbus_value(item_iface.GetValue())
            instance = int(value.split(':')[1])
            logger.info(f"Assigned VRM instance {instance} for {service_type} device {unique_id}")
            return instance
            
    except Exception as e:
        logger.error(f"CRITICAL: Failed to get VRM instance for {service_type} device {unique_id}")
        logger.error(f"Error details: {e}")
        traceback.print_exc()
        raise


def get_json_attribute(data, path):
    """Extract a value from a nested JSON object using dot notation path."""
    parts = path.split('.')
    current = data
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


# ====================================================================
# DbusSwitch Class
# ====================================================================
class DbusSwitch(VeDbusService):
    def __init__(self, service_name, device_config, output_configs, serial_number, mqtt_client,
                 mqtt_on_state_payload, mqtt_off_state_payload, mqtt_on_command_payload, 
                 mqtt_off_command_payload, bus):
        super().__init__(service_name, bus=bus, register=False)

        self.service_name = service_name
        self.device_config = device_config
        self.config_section = device_config.name
        self.serial_number = serial_number
        self.mqtt_on_state_payload_raw = mqtt_on_state_payload
        self.mqtt_off_state_payload_raw = mqtt_off_state_payload
        self.mqtt_on_command_payload = mqtt_on_command_payload
        self.mqtt_off_command_payload = mqtt_off_command_payload
        self.mqtt_on_state_payload_json = None
        self.mqtt_off_state_payload_json = None

        # Parse JSON payloads if they are valid JSON
        try:
            parsed_on = json.loads(mqtt_on_state_payload)
            if isinstance(parsed_on, dict) and len(parsed_on) == 1:
                self.mqtt_on_state_payload_json = parsed_on
        except json.JSONDecodeError:
            pass

        try:
            parsed_off = json.loads(mqtt_off_state_payload)
            if isinstance(parsed_off, dict) and len(parsed_off) == 1:
                self.mqtt_off_state_payload_json = parsed_off
        except json.JSONDecodeError:
            pass

        # Get device instance from settings service
        device_instance = get_vrm_instance(serial_number, 'switch', 0)

        # Add D-Bus paths
        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.19')
        self.add_path('/Mgmt/Connection', 'Virtual')
        self.add_path('/DeviceInstance', device_instance)
        self.add_path('/ProductId', 49257)
        self.add_path('/ProductName', 'Virtual switch')

        # Main service CustomName - saved to 'customname' in config
        main_custom_name = self.device_config.get('CustomName')
        if not main_custom_name:
            parts = self.config_section.split('_')
            if len(parts) >= 3:
                module_idx = parts[1]
                switch_idx = parts[2]
                main_custom_name = f"switch {module_idx}-{switch_idx}"
            else:
                main_custom_name = "Virtual switch"

        self.add_path('/CustomName', main_custom_name, writeable=True, 
                      onchangecallback=self.handle_dbus_change)
        self.add_path('/Serial', serial_number)
        self.add_path('/State', 256)
        self.add_path('/FirmwareVersion', 0)
        self.add_path('/HardwareVersion', 0)
        self.add_path('/Connected', 1)

        self.mqtt_client = mqtt_client

        self.dbus_path_to_state_topic_map = {}
        self.dbus_path_to_command_topic_map = {}
        self.mqtt_subscriptions = set()

        for output_data in output_configs:
            self.add_output(output_data)

        self.register()
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' "
                    f"registered on D-Bus with instance {device_instance}.")

        for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
            if topic:
                self.mqtt_subscriptions.add(topic)
                logger.debug(f"DbusSwitch '{self['/CustomName']}' will subscribe to topic: {topic}")

    def add_output(self, output_data):
        """Add a switchable output to the D-Bus service."""
        output_prefix = f'/SwitchableOutput/output_{output_data["index"]}'
        state_topic = output_data.get('MqttStateTopic')
        command_topic = output_data.get('MqttCommandTopic')
        dbus_state_path = f'{output_prefix}/State'

        if state_topic and 'path/to/mqtt' not in state_topic and \
           command_topic and 'path/to/mqtt' not in command_topic:
            self.dbus_path_to_state_topic_map[dbus_state_path] = state_topic
            self.dbus_path_to_command_topic_map[dbus_state_path] = command_topic
        else:
            logger.warning(f"MQTT topics for {dbus_state_path} in DbusSwitch are invalid. Ignoring.")

        self.add_path(f'{output_prefix}/Name', output_data['name'])
        self.add_path(f'{output_prefix}/Status', 0)
        self.add_path(dbus_state_path, 0, writeable=True, 
                      onchangecallback=self.handle_dbus_change)

        settings_prefix = f'{output_prefix}/Settings'
        self.add_path(f'{settings_prefix}/CustomName', output_data.get('output_custom_name', output_data['name']), 
                      writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path(f'{settings_prefix}/Group', output_data['group'], 
                      writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path(f'{settings_prefix}/Type', 1, writeable=True)
        self.add_path(f'{settings_prefix}/ValidTypes', 7)
        
        # Add ShowUIControl - supports multiple values for different GUI display options
        # 0 = hidden, 1 = show with standard controls, 2 = show with custom controls, etc.
        show_ui_control = output_data.get('ShowUIControl', 1)
        self.add_path(f'{settings_prefix}/ShowUIControl', show_ui_control, 
                      writeable=True, onchangecallback=self.handle_dbus_change)

    def on_mqtt_message_specific(self, client, userdata, msg):
        """Handle MQTT messages specific to this switch."""
        if msg.topic not in self.mqtt_subscriptions:
            return

        logger.debug(f"DbusSwitch specific MQTT callback triggered for {self['/CustomName']} "
                     f"on topic '{msg.topic}'")
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            new_state = None
            
            # Try to parse as JSON
            try:
                incoming_json = json.loads(payload_str)
                if self.mqtt_on_state_payload_json:
                    on_attr, on_val = list(self.mqtt_on_state_payload_json.items())[0]
                    extracted_on_value = get_json_attribute(incoming_json, on_attr)
                    if extracted_on_value is not None and str(extracted_on_value).lower() == str(on_val).lower():
                        new_state = 1
                if new_state is None and self.mqtt_off_state_payload_json:
                    off_attr, off_val = list(self.mqtt_off_state_payload_json.items())[0]
                    extracted_off_value = get_json_attribute(incoming_json, off_attr)
                    if extracted_off_value is not None and str(extracted_off_value).lower() == str(off_val).lower():
                        new_state = 0
                if new_state is None:
                    processed_payload_value = str(incoming_json.get("value", payload_str)).lower()
            except json.JSONDecodeError:
                processed_payload_value = payload_str.lower()

            # If still no state, check raw payloads
            if new_state is None:
                if processed_payload_value == self.mqtt_on_state_payload_raw.lower():
                    new_state = 1
                elif processed_payload_value == self.mqtt_off_state_payload_raw.lower():
                    new_state = 0
                else:
                    logger.warning(f"DbusSwitch: Unrecognized payload '{payload_str}' for topic '{topic}'.")
                    return

            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            if dbus_path and self[dbus_path] != new_state:
                logger.debug(f"DbusSwitch: Updating D-Bus path '{dbus_path}' to {new_state} "
                             f"for '{self['/CustomName']}'.")
                GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, new_state)

        except Exception as e:
            logger.error(f"Error processing MQTT message for DbusSwitch {self.service_name}: {e}")
            traceback.print_exc()

    def handle_dbus_change(self, path, value):
        """Handle D-Bus property changes and save to config."""
        # Handle main service CustomName
        if path == '/CustomName':
            self.save_config_change(self.config_section, 'customname', value)
            return True

        # Handle SwitchableOutput settings
        if "/SwitchableOutput/output_" in path:
            try:
                match = re.search(r'/output_(\d+)/', path)
                if not match:
                    logger.error(f"Failed to parse output index from D-Bus path: {path}")
                    return False

                key_name = path.split('/')[-1]

                if "/State" in path:
                    if value in [0, 1]:
                        self.publish_mqtt_command(path, value)
                        return True
                    return False
                elif "/Settings" in path:
                    if key_name == 'CustomName':
                        self.save_config_change(self.config_section, 'output_customname', value)
                    elif key_name == 'ShowUIControl':
                        # Save ShowUIControl as integer (supports 0, 1, 2, 3, etc.)
                        self.save_config_change(self.config_section, 'showuicontrol', value)
                    else:
                        self.save_config_change(self.config_section, key_name.lower(), value)
                    return True
            except Exception as e:
                logger.error(f"Error handling D-Bus change for switch output {path}: {e}")
                traceback.print_exc()
                return False

        return False

    def save_config_change(self, section, key, value):
        """Save a configuration change to the config file."""
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section):
                config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as configfile:
                config.write(configfile)
            logger.debug(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config file changes for key '{key}': {e}")
            traceback.print_exc()

    def publish_mqtt_command(self, path, value):
        """Publish an MQTT command to control the physical device."""
        if not self.mqtt_client or not self.mqtt_client.is_connected():
            logger.warning(f"MQTT client not connected, cannot publish command for {self.service_name}.")
            return
        if path not in self.dbus_path_to_command_topic_map:
            logger.warning(f"No command topic mapped for D-Bus path '{path}' in {self.service_name}.")
            return
        try:
            command_topic = self.dbus_path_to_command_topic_map[path]
            mqtt_payload = self.mqtt_on_command_payload if value == 1 else self.mqtt_off_command_payload
            self.mqtt_client.publish(command_topic, mqtt_payload, retain=False)
            logger.debug(f"Published MQTT command '{mqtt_payload}' to topic '{command_topic}' "
                         f"for {self.service_name}.")
        except Exception as e:
            logger.error(f"Error during MQTT publish for {self.service_name}: {e}")
            traceback.print_exc()

    def update_dbus_from_mqtt(self, path, value):
        """Update D-Bus path value from MQTT message."""
        try:
            if self[path] != value:
                self[path] = value
                logger.debug(f"DbusSwitch: D-Bus path '{path}' updated to {value}.")
        except Exception as e:
            logger.error(f"Error updating D-Bus path '{path}' in DbusSwitch: {e}")
            traceback.print_exc()
        return False


# ====================================================================
# DbusDigitalInput Class
# ====================================================================
class DbusDigitalInput(VeDbusService):
    DIGITAL_INPUT_TYPES = {
        'disabled': 0,
        'pulse meter': 1,
        'door alarm': 2,
        'bilge pump': 3,
        'bilge alarm': 4,
        'burglar alarm': 5,
        'smoke alarm': 6,
        'fire alarm': 7,
        'co2 alarm': 8,
        'generator': 9,
        'touch input control': 10
    }

    def __init__(self, service_name, device_config, serial_number, mqtt_client, bus):
        super().__init__(service_name, bus=bus, register=False)

        self.device_config = device_config
        self.config_section = device_config.name
        self.serial_number = serial_number
        self.service_name = service_name

        # Get device instance from settings service
        device_instance = get_vrm_instance(serial_number, 'digitalinput', 0)

        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.19')
        self.add_path('/Mgmt/Connection', 'Virtual')
        self.add_path('/DeviceInstance', device_instance)
        self.add_path('/ProductId', 41318)
        self.add_path('/ProductName', 'Virtual digital input')
        self.add_path('/Serial', serial_number)

        self.add_path('/CustomName', self.device_config.get('CustomName', 'Digital Input'), 
                      writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Count', self.device_config.getint('Count', 0), 
                      writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/State', self.device_config.getint('State', 0), 
                      writeable=True, onchangecallback=self.handle_dbus_change)

        initial_type_str = self.device_config.get('Type', 'disabled').lower()
        initial_type_int = self.DIGITAL_INPUT_TYPES.get(initial_type_str, 
                                                         self.DIGITAL_INPUT_TYPES['disabled'])
        self.add_path('/Type', initial_type_int, writeable=True, 
                      onchangecallback=self.handle_dbus_change)

        self.add_path('/Settings/InvertTranslation', 
                      self.device_config.getint('InvertTranslation', 0), 
                      writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Settings/InvertAlarm', 
                      self.device_config.getint('InvertAlarm', 0), 
                      writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Settings/AlarmSetting', 
                      self.device_config.getint('AlarmSetting', 0), 
                      writeable=True, onchangecallback=self.handle_dbus_change)

        self.add_path('/Connected', 1)
        self.add_path('/InputState', 0)
        self.add_path('/Alarm', 0)

        self.mqtt_client = mqtt_client

        self.mqtt_state_topic = self.device_config.get('MqttStateTopic')
        self.mqtt_on_payload = self.device_config.get('mqtt_on_state_payload', 'ON')
        self.mqtt_off_payload = self.device_config.get('mqtt_off_state_payload', 'OFF')

        self.mqtt_subscriptions = set()
        if self.mqtt_state_topic and 'path/to/mqtt' not in self.mqtt_state_topic:
            self.mqtt_subscriptions.add(self.mqtt_state_topic)
            logger.debug(f"DbusDigitalInput '{self['/CustomName']}' will subscribe to topic: "
                         f"{self.mqtt_state_topic}")
        else:
            logger.warning(f"No valid MqttStateTopic for '{self['/CustomName']}'.")

        self.register()
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' "
                    f"registered on D-Bus with instance {device_instance}.")

    def on_mqtt_message_specific(self, client, userdata, msg):
        """Handle MQTT messages specific to this digital input."""
        if msg.topic not in self.mqtt_subscriptions:
            return

        logger.debug(f"DbusDigitalInput specific MQTT callback triggered for "
                     f"{self['/CustomName']} on topic '{msg.topic}'")

        if msg.topic != self.mqtt_state_topic:
            return

        try:
            payload_str = msg.payload.decode().strip()
            logger.debug(f"DbusDigitalInput: Received MQTT message: {payload_str}")

            raw_state = None
            if payload_str.lower() == self.mqtt_on_payload.lower():
                raw_state = 1
            elif payload_str.lower() == self.mqtt_off_payload.lower():
                raw_state = 0

            if raw_state is None:
                logger.warning(f"DbusDigitalInput: Invalid MQTT payload '{payload_str}' received.")
                return

            if self['/InputState'] != raw_state:
                logger.debug(f"DbusDigitalInput: Updating /InputState to {raw_state}")
                GLib.idle_add(self.update_dbus_input_state, raw_state)

            invert = self['/Settings/InvertTranslation']
            final_state = (1 - raw_state) if invert == 1 else raw_state
            dbus_state = self._get_dbus_state_for_type(final_state)

            if self['/State'] != dbus_state:
                logger.debug(f"DbusDigitalInput: Updating /State to {dbus_state}")
                GLib.idle_add(self.update_dbus_state, dbus_state)

        except Exception as e:
            logger.error(f"Error processing MQTT message for Digital Input {self.service_name}: {e}")
            traceback.print_exc()

    def _get_dbus_state_for_type(self, logical_state):
        """Convert logical state to D-Bus state based on input type."""
        current_type = self['/Type']

        if current_type == 2:  # door alarm
            return 7 if logical_state == 1 else 6
        elif current_type == 3:  # bilge pump
            return 3 if logical_state == 1 else 2
        elif 4 <= current_type <= 8:  # various alarms
            return 9 if logical_state == 1 else 8

        return logical_state

    def update_dbus_input_state(self, new_raw_state):
        """Update the InputState D-Bus path."""
        self['/InputState'] = new_raw_state
        return False

    def update_dbus_state(self, new_state_value):
        """Update the State D-Bus path."""
        self['/State'] = new_state_value
        return False

    def handle_dbus_change(self, path, value):
        """Handle D-Bus property changes and save to config."""
        try:
            key_name = path.split('/')[-1]
            logger.debug(f"D-Bus settings change triggered for {path} with value '{value}'.")

            value_to_save = value
            if path == '/Type':
                value_to_save = next((name for name, num in self.DIGITAL_INPUT_TYPES.items() 
                                      if num == value), 'disabled')

            if path.startswith('/Settings/'):
                self.save_config_change(self.config_section, key_name, value)
                if path == '/Settings/InvertTranslation':
                    current_raw_state = self['/InputState']
                    new_invert_setting = value
                    final_state = (1 - current_raw_state) if new_invert_setting == 1 else current_raw_state
                    new_dbus_state = self._get_dbus_state_for_type(final_state)
                    GLib.idle_add(self.update_dbus_state, new_dbus_state)
            else:
                self.save_config_change(self.config_section, key_name, value_to_save)
            return True
        except Exception as e:
            logger.error(f"Failed to handle D-Bus change for {path}: {e}")
            traceback.print_exc()
            return False

    def save_config_change(self, section, key, value):
        """Save a configuration change to the config file."""
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section):
                config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as configfile:
                config.write(configfile)
            logger.debug(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config file changes: {e}")
            traceback.print_exc()


# ====================================================================
# DbusTempSensor Class
# ====================================================================
class DbusTempSensor(VeDbusService):
    TEMPERATURE_TYPES = {
        'battery': 0,
        'fridge': 1,
        'generic': 2,
        'room': 3,
        'outdoor': 4,
        'water heater': 5,
        'freezer': 6
    }

    def __init__(self, service_name, device_config, serial_number, mqtt_client, bus):
        super().__init__(service_name, bus=bus, register=False)

        self.device_config = device_config
        self.config_section = device_config.name
        self.serial_number = serial_number
        self.service_name = service_name

        # Get device instance from settings service
        device_instance = get_vrm_instance(serial_number, 'temperature', 0)

        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.19')
        self.add_path('/Mgmt/Connection', 'Virtual')
        self.add_path('/DeviceInstance', device_instance)
        self.add_path('/ProductId', 49248)
        self.add_path('/ProductName', 'Virtual temperature')
        self.add_path('/CustomName', self.device_config.get('CustomName'), 
                      writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Serial', serial_number)

        self.add_path('/Status', 0)
        self.add_path('/Connected', 1)
        self.add_path('/Temperature', 0.0)

        def is_valid_topic(topic):
            return topic is not None and topic != '' and 'path/to/mqtt' not in topic

        battery_topic = self.device_config.get('BatteryStateTopic')
        if is_valid_topic(battery_topic):
            self.add_path('/BatteryVoltage', 0.0)

        humidity_topic = self.device_config.get('HumidityStateTopic')
        if is_valid_topic(humidity_topic):
            self.add_path('/Humidity', 0.0)

        initial_type_str = self.device_config.get('Type', 'generic').lower()
        initial_type_int = self.TEMPERATURE_TYPES.get(initial_type_str, 
                                                       self.TEMPERATURE_TYPES['generic'])
        self.add_path('/TemperatureType', initial_type_int, writeable=True, 
                      onchangecallback=self.handle_dbus_change)

        self.mqtt_client = mqtt_client

        self.dbus_path_to_state_topic_map = {
            '/Temperature': self.device_config.get('TemperatureStateTopic'),
            '/Humidity': self.device_config.get('HumidityStateTopic'),
            '/BatteryVoltage': self.device_config.get('BatteryStateTopic')
        }

        self.dbus_path_to_state_topic_map = {
            k: v for k, v in self.dbus_path_to_state_topic_map.items()
            if v is not None and v != '' and 'path/to/mqtt' not in v
        }

        self.mqtt_subscriptions = set(self.dbus_path_to_state_topic_map.values())
        for topic in self.mqtt_subscriptions:
            logger.debug(f"DbusTempSensor '{self['/CustomName']}' will subscribe to topic: {topic}")

        self.max_inactivity_seconds = 300
        self.last_valid_update_time = time.time()
        GLib.timeout_add_seconds(self.max_inactivity_seconds // 2, self._check_for_timeout)

        self.register()
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' "
                    f"registered on D-Bus with instance {device_instance}.")

    def _check_for_timeout(self):
        """Check if no data has been received for too long."""
        elapsed = time.time() - self.last_valid_update_time

        if elapsed > self.max_inactivity_seconds and self['/Status'] == 0:
            logger.warning(f"DbusTempSensor: No valid data received for {self['/CustomName']} "
                           f"in {elapsed:.0f} seconds. Setting /Status to 1 (Error).")
            GLib.idle_add(self.update_dbus_from_mqtt, '/Status', 1)

        return True

    def on_mqtt_message_specific(self, client, userdata, msg):
        """Handle MQTT messages specific to this temperature sensor."""
        if msg.topic not in self.mqtt_subscriptions:
            return

        logger.debug(f"DbusTempSensor specific MQTT callback triggered for "
                     f"{self['/CustomName']} on topic '{msg.topic}'")
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() 
                              if v == topic), None)

            if not dbus_path:
                logger.debug(f"DbusTempSensor: Received message on non-matching topic '{msg.topic}'.")
                return

            value = None
            try:
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json:
                    value = float(incoming_json["value"])
                else:
                    logger.warning(f"DbusTempSensor: JSON payload for topic '{topic}' "
                                   f"does not contain 'value' key.")
                    return
            except json.JSONDecodeError:
                try:
                    value = float(payload_str)
                except ValueError:
                    logger.warning(f"DbusTempSensor: Payload '{payload_str}' for topic '{topic}' "
                                   f"is not valid float or JSON.")
                    return

            if value is None:
                logger.warning(f"DbusTempSensor: Could not extract valid numerical value from payload.")
                return

            self.last_valid_update_time = time.time()
            if self['/Status'] != 0:
                GLib.idle_add(self.update_dbus_from_mqtt, '/Status', 0)

            if self[dbus_path] != value:
                logger.debug(f"DbusTempSensor: Updating D-Bus path '{dbus_path}' to {value} "
                             f"for '{self['/CustomName']}'.")
                GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)

        except Exception as e:
            logger.error(f"Error processing MQTT message for TempSensor {self.service_name}: {e}")
            traceback.print_exc()

    def handle_dbus_change(self, path, value):
        """Handle D-Bus property changes and save to config."""
        if path == '/CustomName':
            self.save_config_change(self.config_section, 'customname', value)
            return True
        elif path == '/TemperatureType':
            type_str = next((k for k, v in self.TEMPERATURE_TYPES.items() if v == value), 'generic')
            self.save_config_change(self.config_section, 'type', type_str)
            return True
        return False

    def save_config_change(self, section, key, value):
        """Save a configuration change to the config file."""
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section):
                config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as configfile:
                config.write(configfile)
            logger.debug(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config file changes for TempSensor key '{key}': {e}")
            traceback.print_exc()

    def update_dbus_from_mqtt(self, path, value):
        """Update D-Bus path value from MQTT message."""
        self[path] = value
        return False


# ====================================================================
# DbusTankSensor Class
# ====================================================================
class DbusTankSensor(VeDbusService):
    FLUID_TYPES = {
        'fuel': 0, 'fresh water': 1, 'waste water': 2, 'live well': 3, 'oil': 4,
        'black water': 5, 'gasoline': 6, 'diesel': 7, 'lpg': 8, 'lng': 9,
        'hydraulic oil': 10, 'raw water': 11
    }

    def __init__(self, service_name, device_config, serial_number, mqtt_client, bus):
        super().__init__(service_name, bus=bus, register=False)
        self.device_config = device_config
        self.config_section = device_config.name
        self.serial_number = serial_number
        self.service_name = service_name

        # Get device instance from settings service
        device_instance = get_vrm_instance(serial_number, 'tank', 0)

        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.19')
        self.add_path('/Mgmt/Connection', 'Virtual')
        self.add_path('/DeviceInstance', device_instance)
        self.add_path('/ProductId', 49251)
        self.add_path('/ProductName', 'Virtual tank')
        self.add_path('/CustomName', self.device_config.get('CustomName'), 
                      writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Serial', serial_number)

        self.add_path('/Status', 0)
        self.add_path('/Connected', 1)
        self.add_path('/Capacity', self.device_config.getfloat('Capacity', 0.2), 
                      writeable=True, onchangecallback=self.handle_dbus_change)

        initial_fluid_type_str = self.device_config.get('FluidType', 'fresh water').lower()
        initial_fluid_type_int = self.FLUID_TYPES.get(initial_fluid_type_str, 
                                                       self.FLUID_TYPES['fresh water'])
        self.add_path('/FluidType', initial_fluid_type_int, writeable=True, 
                      onchangecallback=self.handle_dbus_change)

        self.add_path('/Level', 0.0)
        self.add_path('/Remaining', 0.0)
        self.add_path('/RawValue', 0.0)
        self.add_path('/RawValueEmpty', self.device_config.getfloat('RawValueEmpty', 0.0), 
                      writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/RawValueFull', self.device_config.getfloat('RawValueFull', 0.0), 
                      writeable=True, onchangecallback=self.handle_dbus_change)

        self.add_path('/RawUnit', self.device_config.get('RawUnit', ''))
        self.add_path('/Shape', 0)

        self.mqtt_client = mqtt_client

        self.dbus_path_to_state_topic_map = {}
        self.is_level_direct = False

        def is_valid_topic(topic):
            return topic and 'path/to/mqtt' not in topic

        level_topic = self.device_config.get('LevelStateTopic')
        raw_topic = self.device_config.get('RawValueStateTopic')

        if is_valid_topic(raw_topic):
            self.dbus_path_to_state_topic_map['/RawValue'] = raw_topic
            logger.debug(f"Tank '{self['/CustomName']}' will use RawValue topic: {raw_topic}")
        elif is_valid_topic(level_topic):
            self.is_level_direct = True
            self.dbus_path_to_state_topic_map['/Level'] = level_topic
            logger.debug(f"Tank '{self['/CustomName']}' will use direct Level topic: {level_topic}")
        else:
            logger.warning(f"Tank '{self['/CustomName']}': Neither RawValueStateTopic "
                           f"nor LevelStateTopic are valid.")

        temp_topic = self.device_config.get('TemperatureStateTopic')
        if is_valid_topic(temp_topic):
            self.add_path('/Temperature', 0.0)
            self.dbus_path_to_state_topic_map['/Temperature'] = temp_topic
            logger.debug(f"Tank '{self['/CustomName']}' also subscribing to Temperature topic: "
                         f"{temp_topic}")

        battery_topic = self.device_config.get('BatteryStateTopic')
        if is_valid_topic(battery_topic):
            self.add_path('/BatteryVoltage', 0.0)
            self.dbus_path_to_state_topic_map['/BatteryVoltage'] = battery_topic
            logger.debug(f"Tank '{self['/CustomName']}' also subscribing to BatteryVoltage topic: "
                         f"{battery_topic}")

        self.mqtt_subscriptions = set(self.dbus_path_to_state_topic_map.values())
        for topic in self.mqtt_subscriptions:
            logger.debug(f"DbusTankSensor '{self['/CustomName']}' will subscribe to topic: {topic}")

        self.max_inactivity_seconds = 300
        self.last_valid_update_time = time.time()
        GLib.timeout_add_seconds(self.max_inactivity_seconds // 2, self._check_for_timeout)

        self.register()
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' "
                    f"registered on D-Bus with instance {device_instance}.")

        if not self.is_level_direct:
            self._calculate_level_from_raw_value()
        self._calculate_remaining_from_level()

    def _check_for_timeout(self):
        """Check if no data has been received for too long."""
        elapsed = time.time() - self.last_valid_update_time

        if elapsed > self.max_inactivity_seconds and self['/Status'] == 0:
            logger.warning(f"DbusTankSensor: No valid data received for {self['/CustomName']} "
                           f"in {elapsed:.0f} seconds. Setting /Status to 1 (Error).")
            GLib.idle_add(self.update_dbus_from_mqtt, '/Status', 1)

        return True

    def on_mqtt_message_specific(self, client, userdata, msg):
        """Handle MQTT messages specific to this tank sensor."""
        if msg.topic not in self.mqtt_subscriptions:
            return

        logger.debug(f"DbusTankSensor specific MQTT callback triggered for "
                     f"{self['/CustomName']} on topic '{msg.topic}'")
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() 
                              if v == topic), None)
            if not dbus_path:
                logger.debug(f"DbusTankSensor: Received message on non-matching topic '{msg.topic}'.")
                return

            value = None
            try:
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json:
                    value = float(incoming_json["value"])
                else:
                    logger.warning(f"DbusTankSensor: JSON payload for topic '{topic}' "
                                   f"does not contain 'value' key.")
                    return
            except json.JSONDecodeError:
                try:
                    value = float(payload_str)
                except ValueError:
                    logger.warning(f"DbusTankSensor: Payload '{payload_str}' for topic '{topic}' "
                                   f"is not valid float or JSON.")
                    return

            if value is None:
                logger.warning(f"DbusTankSensor: Could not extract valid numerical value.")
                return

            self.last_valid_update_time = time.time()

            if dbus_path == '/RawValue' and not self.is_level_direct:
                if self['/RawValue'] != value:
                    logger.debug(f"DbusTankSensor: Updating /RawValue to {value} and recalculating.")
                    GLib.idle_add(self._update_raw_value_and_recalculate, value)
            elif dbus_path == '/Level' and self.is_level_direct:
                if 0.0 <= value <= 100.0 and self['/Level'] != round(value, 2):
                    logger.debug(f"DbusTankSensor: Updating /Level to {value} and recalculating.")
                    GLib.idle_add(self._update_level_and_recalculate, value)
            else:
                if self[dbus_path] != value:
                    logger.debug(f"DbusTankSensor: Updating D-Bus path '{dbus_path}' to {value}.")
                    GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)

        except Exception as e:
            logger.error(f"Error processing MQTT message for Tank {self.service_name}: {e}")
            traceback.print_exc()

    def _update_raw_value_and_recalculate(self, raw_value):
        """Update raw value and recalculate level and remaining."""
        self['/RawValue'] = raw_value
        self._calculate_level_from_raw_value()
        self._calculate_remaining_from_level()
        if self['/Status'] != 0:
            self['/Status'] = 0
        return False

    def _update_level_and_recalculate(self, level_value):
        """Update level directly and recalculate remaining."""
        if 0.0 <= level_value <= 100.0:
            self['/Level'] = round(level_value, 2)
            self._calculate_remaining_from_level()
            if self['/Status'] != 0:
                self['/Status'] = 0
        return False

    def _calculate_level_from_raw_value(self):
        """Calculate level percentage from raw value."""
        raw_value = self['/RawValue']
        raw_empty = self['/RawValueEmpty']
        raw_full = self['/RawValueFull']
        level = 0.0
        if raw_full != raw_empty:
            level = ((raw_value - raw_empty) / (raw_full - raw_empty)) * 100.0
            level = max(0.0, min(100.0, level))
        self['/Level'] = round(level, 2)
        logger.debug(f"Tank '{self['/CustomName']}' calculated Level: {self['/Level']}")

    def _calculate_remaining_from_level(self):
        """Calculate remaining volume from level percentage and capacity."""
        remaining = (self['/Level'] / 100.0) * self['/Capacity']
        self['/Remaining'] = round(remaining, 2)
        logger.debug(f"Tank '{self['/CustomName']}' calculated Remaining: {self['/Remaining']}")

    def handle_dbus_change(self, path, value):
        """Handle D-Bus property changes and save to config."""
        key_name = path.split('/')[-1]

        value_to_save = value
        if key_name == 'FluidType':
            value_to_save = next((k for k, v in self.FLUID_TYPES.items() if v == value), 'fresh water')
            logger.debug(f"Tank: Converting FluidType {value} to string '{value_to_save}' for saving.")

        self.save_config_change(self.config_section, key_name, value_to_save)

        if path in ['/RawValueEmpty', '/RawValueFull'] and not self.is_level_direct:
            GLib.idle_add(self._calculate_level_from_raw_value)
            GLib.idle_add(self._calculate_remaining_from_level)
        elif path == '/Capacity':
            GLib.idle_add(self._calculate_remaining_from_level)

        return True

    def save_config_change(self, section, key, value):
        """Save a configuration change to the config file."""
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section):
                config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as f:
                config.write(f)
            logger.debug(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config change for Tank: {e}")
            traceback.print_exc()

    def update_dbus_from_mqtt(self, path, value):
        """Update D-Bus path value from MQTT message."""
        self[path] = value
        if path != '/Status' and self['/Status'] != 0:
            self['/Status'] = 0
        return False


# ====================================================================
# DbusBattery Class
# ====================================================================
class DbusBattery(VeDbusService):
    def __init__(self, service_name, device_config, serial_number, mqtt_client, bus):
        super().__init__(service_name, bus=bus, register=False)
        self.device_config = device_config
        self.config_section = device_config.name
        self.serial_number = serial_number
        self.service_name = service_name

        # Get device instance from settings service
        device_instance = get_vrm_instance(serial_number, 'battery', 0)

        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.19')
        self.add_path('/Mgmt/Connection', 'Virtual')
        self.add_path('/DeviceInstance', device_instance)
        self.add_path('/ProductId', 49253)
        self.add_path('/ProductName', 'Virtual battery')
        self.add_path('/CustomName', self.device_config.get('CustomName'), 
                      writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Serial', serial_number)

        self.add_path('/Connected', 1)
        self.add_path('/Soc', 0.0)
        self.add_path('/Soh', 0.0)
        self.add_path('/Capacity', self.device_config.getfloat('CapacityAh'), 
                      writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Dc/0/Current', 0.0)
        self.add_path('/Dc/0/Power', 0.0)
        self.add_path('/Dc/0/Temperature', 0.0)
        self.add_path('/Dc/0/Voltage', 0.0)

        self.add_path('/ErrorCode', 0)
        self.add_path('/Info/MaxChargeCurrent', 0)
        self.add_path('/Info/MaxDischargeCurrent', 0)
        self.add_path('/Info/MaxChargeVoltage', 0.)

        self.mqtt_client = mqtt_client

        self.dbus_path_to_state_topic_map = {
            '/Dc/0/Current': self.device_config.get('CurrentStateTopic'),
            '/Dc/0/Power': self.device_config.get('PowerStateTopic'),
            '/Dc/0/Temperature': self.device_config.get('TemperatureStateTopic'),
            '/Dc/0/Voltage': self.device_config.get('VoltageStateTopic'),
            '/Soc': self.device_config.get('SocStateTopic'),
            '/Soh': self.device_config.get('SohStateTopic'),
            '/Info/MaxChargeCurrent': self.device_config.get('MaxChargeCurrentStateTopic'),
            '/Info/MaxDischargeCurrent': self.device_config.get('MaxDischargeCurrentStateTopic'),
            '/Info/MaxChargeVoltage': self.device_config.get('MaxChargeVoltageStateTopic'),
        }
        self.dbus_path_to_state_topic_map = {k: v for k, v in self.dbus_path_to_state_topic_map.items() 
                                              if v and 'path/to/mqtt' not in v}

        self.mqtt_subscriptions = set(self.dbus_path_to_state_topic_map.values())
        for topic in self.mqtt_subscriptions:
            logger.debug(f"DbusBattery '{self['/CustomName']}' will subscribe to topic: {topic}")

        self.register()
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' "
                    f"registered on D-Bus with instance {device_instance}.")

    def on_mqtt_message_specific(self, client, userdata, msg):
        """Handle MQTT messages specific to this battery."""
        if msg.topic not in self.mqtt_subscriptions:
            return

        logger.debug(f"DbusBattery specific MQTT callback triggered for "
                     f"{self['/CustomName']} on topic '{msg.topic}'")
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() 
                              if v == topic), None)
            if not dbus_path:
                logger.debug(f"DbusBattery: Received message on non-matching topic.")
                return

            value = None
            try:
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json:
                    value = incoming_json["value"]
                else:
                    logger.warning(f"DbusBattery: JSON payload does not contain 'value' key.")
                    return
            except json.JSONDecodeError:
                try:
                    value = float(payload_str)
                except ValueError:
                    logger.warning(f"DbusBattery: Payload '{payload_str}' is not valid float or JSON.")
                    return

            if value is None:
                logger.warning(f"DbusBattery: Could not extract valid numerical value.")
                return

            if self[dbus_path] != value:
                logger.debug(f"DbusBattery: Updating D-Bus path '{dbus_path}' to {value}.")
                GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)

        except Exception as e:
            logger.error(f"Error processing MQTT message for Battery {self.service_name}: {e}")
            traceback.print_exc()

    def handle_dbus_change(self, path, value):
        """Handle D-Bus property changes and save to config."""
        if path == '/CustomName':
            self.save_config_change(self.config_section, 'customname', value)
            return True
        elif path == '/Capacity':
            self.save_config_change(self.config_section, 'capacityah', value)
            return True
        return False

    def save_config_change(self, section, key, value):
        """Save a configuration change to the config file."""
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section):
                config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as f:
                config.write(f)
            logger.debug(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config change for Battery: {e}")
            traceback.print_exc()

    def update_dbus_from_mqtt(self, path, value):
        """Update D-Bus path value from MQTT message."""
        self[path] = value
        return False


# ====================================================================
# DbusPvCharger Class
# ====================================================================
class DbusPvCharger(VeDbusService):
    def __init__(self, service_name, device_config, serial_number, mqtt_client, bus):
        super().__init__(service_name, bus=bus, register=False)
        self.device_config = device_config
        self.config_section = device_config.name
        self.serial_number = serial_number
        self.service_name = service_name

        # Get device instance from settings service
        device_instance = get_vrm_instance(serial_number, 'solarcharger', 0)

        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.0.1')
        self.add_path('/Mgmt/Connection', 'Virtual')
        self.add_path('/DeviceInstance', device_instance)
        self.add_path('/ProductId', 41318)
        self.add_path('/ProductName', 'Virtual MPPT')
        self.add_path('/CustomName', self.device_config.get('CustomName'), 
                      writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Serial', serial_number)

        self.add_path('/Connected', 1)

        self.add_path('/Dc/0/Current', 0.0)
        self.add_path('/Dc/0/Voltage', 0.0)

        self.add_path('/Link/ChargeVoltage', None)
        self.add_path('/Link/ChargeCurrent', None)

        self.add_path('/Load/State', None)

        self.add_path('/State', 0)

        self.add_path('/Pv/V', 0.0)
        self.add_path('/Yield/Power', 0.0)
        self.add_path('/Yield/User', 0.0)
        self.add_path('/Yield/System', 0.0)

        self.mqtt_client = mqtt_client

        self.dbus_path_to_state_topic_map = {
            '/Dc/0/Current': self.device_config.get('BatteryCurrentStateTopic'),
            '/Dc/0/Voltage': self.device_config.get('BatteryVoltageStateTopic'),
            '/Link/ChargeVoltage': self.device_config.get('MaxChargeVoltageStateTopic'),
            '/Link/ChargeCurrent': self.device_config.get('MaxChargeCurrentStateTopic'),
            '/Load/State': self.device_config.get('LoadStateTopic'),
            '/State': self.device_config.get('ChargerStateTopic'),
            '/Pv/V': self.device_config.get('PvVoltageStateTopic'),
            '/Yield/Power': self.device_config.get('PvPowerStateTopic'),
            '/Yield/User': self.device_config.get('TotalYield'),
            '/Yield/System': self.device_config.get('SystemYield')
        }
        self.dbus_path_to_state_topic_map = {k: v for k, v in self.dbus_path_to_state_topic_map.items() 
                                              if v and 'path/to/mqtt' not in v}

        self.mqtt_subscriptions = set(self.dbus_path_to_state_topic_map.values())
        for topic in self.mqtt_subscriptions:
            logger.debug(f"DbusPvCharger '{self['/CustomName']}' will subscribe to topic: {topic}")

        self.register()
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' "
                    f"registered on D-Bus with instance {device_instance}.")

    def on_mqtt_message_specific(self, client, userdata, msg):
        """Handle MQTT messages specific to this PV charger."""
        if msg.topic not in self.mqtt_subscriptions:
            return

        logger.debug(f"DbusPvCharger specific MQTT callback triggered for "
                     f"{self['/CustomName']} on topic '{msg.topic}'")
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() 
                              if v == topic), None)
            if not dbus_path:
                return

            value = None
            try:
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json:
                    value = incoming_json["value"]
                else:
                    value = float(payload_str)
            except (json.JSONDecodeError, ValueError):
                if dbus_path == '/State':
                    state_map = {'off': 0, 'bulk': 3, 'absorption': 4, 'float': 5}
                    try:
                        value = int(payload_str)
                    except ValueError:
                        value = state_map.get(payload_str.lower())
                elif dbus_path == '/Load/State':
                    state_map = {'off': 0, 'on': 1}
                    try:
                        value = int(payload_str)
                    except ValueError:
                        value = state_map.get(payload_str.lower())
                else:
                    try:
                        value = float(payload_str)
                    except ValueError:
                        logger.warning(f"DbusPvCharger: Payload '{payload_str}' is not valid.")
                        return

            if value is None:
                logger.warning(f"DbusPvCharger: Could not extract valid value.")
                return

            if self[dbus_path] != value:
                logger.debug(f"DbusPvCharger: Updating D-Bus path '{dbus_path}' to {value}.")
                GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)

        except Exception as e:
            logger.error(f"Error processing MQTT message for PV Charger {self.service_name}: {e}")
            traceback.print_exc()

    def handle_dbus_change(self, path, value):
        """Handle D-Bus property changes and save to config."""
        if path == '/CustomName':
            self.save_config_change(self.config_section, 'customname', value)
            return True
        return False

    def save_config_change(self, section, key, value):
        """Save a configuration change to the config file."""
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section):
                config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as f:
                config.write(f)
            logger.debug(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config change for PV Charger: {e}")
            traceback.print_exc()

    def update_dbus_from_mqtt(self, path, value):
        """Update D-Bus path value from MQTT message."""
        if isinstance(value, (float, int)):
            self[path] = round(value, 2)
        else:
            self[path] = value
        return False


# ====================================================================
# Global MQTT Callbacks
# ====================================================================
active_services = []


def on_mqtt_connect_global(client, userdata, flags, rc, properties):
    """Global MQTT connect callback."""
    if rc == 0:
        logger.info("Successfully connected to MQTT Broker!")
        if userdata:
            logger.info("Re-subscribing to topics...")
            for topic in userdata:
                client.subscribe(topic)
                logger.debug(f"Subscribed to topic: {topic}")
    else:
        logger.error(f"Failed to connect to MQTT Broker, return code {rc}")


def on_mqtt_message_dispatcher(client, userdata, msg):
    """Global MQTT message dispatcher - sends messages to all services."""
    logger.debug(f"GLOBAL MQTT MESSAGE RECEIVED: Topic='{msg.topic}'")
    for service in active_services:
        service.on_mqtt_message_specific(client, userdata, msg)


def on_mqtt_disconnect(client, userdata, rc, properties=None, reason=None):
    """Global MQTT disconnect callback."""
    logger.warning(f"MQTT client disconnected with result code: {rc}")


def on_mqtt_subscribe(client, userdata, mid, granted_qos, properties=None):
    """Global MQTT subscribe callback."""
    logger.debug(f"MQTT Subscription acknowledged by broker. Message ID: {mid}")


# ====================================================================
# Main Function
# ====================================================================
def main():
    global active_services

    logger.info("Starting D-Bus Virtual Devices main service.")

    from dbus.mainloop.glib import DBusGMainLoop
    DBusGMainLoop(set_as_default=True)

    # Read configuration
    config = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE_PATH):
        logger.critical(f"Config file not found: {CONFIG_FILE_PATH}")
        sys.exit(1)

    try:
        config.read(CONFIG_FILE_PATH)
    except configparser.Error as e:
        logger.critical(f"Error parsing config file: {e}")
        sys.exit(1)

    # Set log level
    log_level = logging.INFO
    if config.has_section('Global'):
        log_level_str = config['Global'].get('LogLevel', 'INFO').upper()
        log_level = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 
                     'WARNING': logging.WARNING, 'ERROR': logging.ERROR}.get(log_level_str, logging.INFO)
    logger.setLevel(log_level)

    # MQTT configuration
    mqtt_config = config['MQTT'] if config.has_section('MQTT') else {}
    MQTT_HOST = mqtt_config.get('BrokerAddress', 'localhost')
    MQTT_PORT = mqtt_config.getint('Port', 1883)
    MQTT_USERNAME = mqtt_config.get('Username')
    MQTT_PASSWORD = mqtt_config.get('Password')

    # Setup MQTT client
    client_id = f"external-devices-main-script-{os.getpid()}"
    logger.info(f"Using MQTT Client ID: {client_id}")
    mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)

    all_topics_to_subscribe = set()
    mqtt_client.user_data_set(all_topics_to_subscribe)

    mqtt_client.on_connect = on_mqtt_connect_global
    mqtt_client.on_message = on_mqtt_message_dispatcher
    mqtt_client.on_subscribe = on_mqtt_subscribe
    mqtt_client.on_disconnect = on_mqtt_disconnect

    if MQTT_USERNAME and MQTT_PASSWORD:
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        logger.debug("MQTT Username/Password set.")

    # Device type mapping
    device_type_map = {
        'temp_sensor_': DbusTempSensor,
        'tank_sensor_': DbusTankSensor,
        'virtual_battery_': DbusBattery,
        'input_': DbusDigitalInput,
        'pv_charger_': DbusPvCharger
    }

    # Process each config section
    sections_to_process = []
    for section in config.sections():
        section_lower = section.lower()
        if section_lower in ['global', 'mqtt']:
            continue
        sections_to_process.append(section)

    for section in sections_to_process:
        section_lower = section.lower()

        # Handle switch sections
        if section_lower.startswith('switch_') and re.match(r'^switch_\d+_\d+$', section_lower):
            try:
                device_config = config[section]

                parts = section.split('_')
                module_idx = parts[1]
                switch_idx = parts[2]

                parent_module_section = f'Relay_Module_{module_idx}'
                parent_config = config[parent_module_section] if config.has_section(parent_module_section) else {}

                serial_number = device_config.get('Serial')
                if not serial_number:
                    logger.critical(f"Serial number not found for [{section}]. "
                                    f"Cannot create service without serial number.")
                    sys.exit(1)

                device_bus = dbus.bus.BusConnection(dbus.Bus.TYPE_SYSTEM)

                service_name = f'com.victronenergy.switch.external_{serial_number}'

                # Get output custom name from config
                output_custom_name = device_config.get('output_customname', 
                                                        device_config.get('CustomName', 
                                                        f'switch {module_idx}-{switch_idx}'))

                output_configs = [{
                    'index': int(switch_idx),
                    'name': f'{module_idx}-{switch_idx}',
                    'output_custom_name': output_custom_name,
                    'group': device_config.get('Group', f'Group{module_idx}'),
                    'MqttStateTopic': device_config.get('MqttStateTopic'),
                    'MqttCommandTopic': device_config.get('MqttCommandTopic'),
                    'ShowUIControl': device_config.getint('showuicontrol', fallback=1)
                }]

                mqtt_on_state = parent_config.get('mqtt_on_state_payload', 'ON')
                mqtt_off_state = parent_config.get('mqtt_off_state_payload', 'OFF')
                mqtt_on_cmd = parent_config.get('mqtt_on_command_payload', 'ON')
                mqtt_off_cmd = parent_config.get('mqtt_off_command_payload', 'OFF')

                service = DbusSwitch(
                    service_name, device_config, output_configs, serial_number, mqtt_client,
                    mqtt_on_state, mqtt_off_state, mqtt_on_cmd, mqtt_off_cmd, device_bus
                )

                active_services.append(service)
                logger.debug(f"Successfully initialized D-Bus service for switch section [{section}]")

                all_topics_to_subscribe.update(service.mqtt_subscriptions)

            except Exception as e:
                logger.error(f"Failed to initialize D-Bus service for switch section [{section}]: {e}")
                traceback.print_exc()
                sys.exit(1)

        else:
            # Handle other device types
            device_class = None
            device_type_string = None

            for prefix, cls in device_type_map.items():
                if section_lower.startswith(prefix):
                    device_class = cls
                    device_type_string = prefix.strip('_')
                    logger.debug(f"Section '{section}' matched device type '{device_type_string}'.")
                    break

            if device_class:
                try:
                    device_config = config[section]

                    serial_number = device_config.get('Serial')
                    if not serial_number:
                        logger.critical(f"Serial number not found for [{section}]. "
                                        f"Cannot create service without serial number.")
                        sys.exit(1)

                    device_bus = dbus.bus.BusConnection(dbus.Bus.TYPE_SYSTEM)

                    base_service_name_type = device_type_string.replace("_", "")
                    if base_service_name_type == 'input':
                        base_service_name_type = 'digitalinput'
                    elif base_service_name_type == 'tanksensor':
                        base_service_name_type = 'tank'
                    elif base_service_name_type == 'tempsensor':
                        base_service_name_type = 'temperature'
                    elif base_service_name_type == 'virtualbattery':
                        base_service_name_type = 'battery'
                    elif base_service_name_type == 'pvcharger':
                        base_service_name_type = 'solarcharger'

                    service_name = f'com.victronenergy.{base_service_name_type}.external_{serial_number}'

                    service = device_class(service_name, device_config, serial_number, mqtt_client, device_bus)

                    active_services.append(service)
                    logger.debug(f"Successfully initialized D-Bus service for [{section}] "
                                 f"of type '{device_type_string}'.")

                    all_topics_to_subscribe.update(service.mqtt_subscriptions)

                except Exception as e:
                    logger.error(f"Failed to initialize D-Bus service for [{section}] "
                                 f"({device_type_string}): {e}")
                    traceback.print_exc()
                    sys.exit(1)
            elif not section_lower.startswith('relay_module_'):
                logger.debug(f"Section '{section}' does not match any known device type prefix. Skipping.")

    # Connect to MQTT broker
    try:
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        mqtt_client.loop_start()
        logger.info(f"Connecting to MQTT broker at {MQTT_HOST}:{MQTT_PORT}...")
    except Exception as e:
        logger.critical(f"Initial connection to MQTT broker failed: {e}. Exiting.")
        traceback.print_exc()
        sys.exit(1)

    # Check if any services were started
    if not active_services:
        logger.warning("No device services were started. Exiting.")
        if mqtt_client:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        sys.exit(0)

    # Start main loop
    logger.info('All identified external device services created. Starting GLib.MainLoop().')

    mainloop = GLib.MainLoop()
    try:
        mainloop.run()
    except KeyboardInterrupt:
        logger.debug("Exiting D-Bus Virtual Devices main service.")
    except Exception as e:
        logger.error(f"An unexpected error occurred in the main loop: {e}")
        traceback.print_exc()
    finally:
        if mqtt_client:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            logger.debug("MQTT client disconnected.")
        logger.debug("Script finished.")


if __name__ == "__main__":
    main()
