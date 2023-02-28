# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

# VSM is Volumetric Soil Moisture
import os
import asyncio
import random
import logging

from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device.aio import ProvisioningDeviceClient
from azure.iot.device import MethodResponse
from datetime import timedelta, datetime
import pnp_helper

logging.basicConfig(level=logging.ERROR)

# The device "MoistureController" that is getting implemented using the above interfaces.
# This id can change according to the company the user is from
# and the name user wants to call this Plug and Play device
model_id = "dtmi:stemnetiot:MoistureController_1kc;1"

# the components inside this Plug and Play device.
# there can be multiple components from 1 interface
# component names according to interfaces following pascal case.
device_information_component_name = "deviceInformation"
sensor_1_component_name = "sensor1"
sensor_2_component_name = "sensor2"
serial_number = "some_serial_number"
#####################################################
# COMMAND HANDLERS : User will define these handlers
# depending on what commands the component defines

#####################################################
# GLOBAL VARIABLES
SENSOR_1 = None
SENSOR_2 = None


class Sensor(object):
    def __init__(self, name, moving_win=10):

        self.moving_window = moving_win
        self.records = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        self.index = 0

        self.cur = 0
        self.max = 0
        self.min = 0
        self.avg = 0

        self.name = name

    def record(self, current_vsm):
        self.cur = current_vsm
        self.records[self.index] = current_vsm
        self.max = self.calculate_max(current_vsm)
        self.min = self.calculate_min(current_vsm)
        self.avg = self.calculate_average()

        self.index = (self.index + 1) % self.moving_window

    def calculate_max(self, current_vsm):
        if not self.max:
            return current_vsm
        elif current_vsm > self.max:
            return self.max

    def calculate_min(self, current_vsm):
        if not self.min:
            return current_vsm
        elif current_vsm < self.min:
            return self.min

    def calculate_average(self):
        return sum(self.records) / self.moving_window

    def create_report(self):
        response_dict = {}
        response_dict["maxVMS"] = self.max
        response_dict["minVMS"] = self.min
        response_dict["avgVMS"] = self.avg
        response_dict["startTime"] = (
            (datetime.now() - timedelta(0, self.moving_window * 8)).astimezone().isoformat()
        )
        response_dict["endTime"] = datetime.now().astimezone().isoformat()
        return response_dict


async def reboot_handler(values):
    if values:
        print("Rebooting after delay of {delay} secs".format(delay=values))
    print("Done rebooting")


async def max_min_handler(values):
    if values:
        print(
            "Will return the max, min and average vsmerature from the specified time {since} to the current time".format(
                since=values
            )
        )
    print("Done generating")


# END COMMAND HANDLERS
#####################################################

#####################################################
# CREATE RESPONSES TO COMMANDS


def create_max_min_report_response(sensor_name):
    """
    An example function that can create a response to the "getMaxMinReport" command request the way the user wants it.
    Most of the times response is created by a helper function which follows a generic pattern.
    This should be only used when the user wants to give a detailed response back to the Hub.
    :param values: The values that were received as part of the request.
    """
    if "Sensor;1" in sensor_name and SENSOR_1:
        response_dict = SENSOR_1.create_report()
    elif SENSOR_2:
        response_dict = SENSOR_2.create_report()
    else:  # This is done to pass certification.
        response_dict = {}
        response_dict["maxVMS"] = 0
        response_dict["minVMS"] = 0
        response_dict["avgVMS"] = 0
        response_dict["startTime"] = datetime.now().astimezone().isoformat()
        response_dict["endTime"] = datetime.now().astimezone().isoformat()

    print(response_dict)
    return response_dict


# END CREATE RESPONSES TO COMMANDS
#####################################################

#####################################################
# TELEMETRY TASKS


async def send_telemetry_from_vsm_controller(device_client, telemetry_msg, component_name=None):
    msg = pnp_helper.create_telemetry(telemetry_msg, component_name)
    await device_client.send_message(msg)
    print("Sent message")
    print(msg)
    await asyncio.sleep(5)


#####################################################
# COMMAND TASKS


async def execute_command_listener(
    device_client,
    component_name=None,
    method_name=None,
    user_command_handler=None,
    create_user_response_handler=None,
):
    """
    Coroutine for executing listeners. These will listen for command requests.
    They will take in a user provided handler and call the user provided handler
    according to the command request received.
    :param device_client: The device client
    :param component_name: The name of the device like "sensor"
    :param method_name: (optional) The specific method name to listen for. Eg could be "blink", "turnon" etc.
    If not provided the listener will listen for all methods.
    :param user_command_handler: (optional) The user provided handler that needs to be executed after receiving "command requests".
    If not provided nothing will be executed on receiving command.
    :param create_user_response_handler: (optional) The user provided handler that will create a response.
    If not provided a generic response will be created.
    :return:
    """
    while True:
        if component_name and method_name:
            command_name = component_name + "*" + method_name
        elif method_name:
            command_name = method_name
        else:
            command_name = None

        command_request = await device_client.receive_method_request(command_name)
        print("Command request received with payload")
        values = command_request.payload
        print(values)

        if user_command_handler:
            await user_command_handler(values)
        else:
            print("No handler provided to execute")

        (response_status, response_payload) = pnp_helper.create_response_payload_with_status(
            command_request, method_name, create_user_response=create_user_response_handler
        )

        command_response = MethodResponse.create_from_method_request(
            command_request, response_status, response_payload
        )

        try:
            await device_client.send_method_response(command_response)
        except Exception:
            print("responding to the {command} command failed".format(command=method_name))


#####################################################
# PROPERTY TASKS


async def execute_property_listener(device_client):
    while True:
        patch = await device_client.receive_twin_desired_properties_patch()  # blocking call
        print(patch)
        properties_dict = pnp_helper.create_reported_properties_from_desired(patch)

        await device_client.patch_twin_reported_properties(properties_dict)


#####################################################
# An # END KEYBOARD INPUT LISTENER to quit application


def stdin_listener():
    """
    Listener for quitting the sample
    """
    while True:
        selection = input("Press Q to quit\n")
        if selection == "Q" or selection == "q":
            print("Quitting...")
            break


# END KEYBOARD INPUT LISTENER
#####################################################


#####################################################
# MAIN STARTS
async def provision_device(provisioning_host, id_scope, registration_id, symmetric_key, model_id):
    provisioning_device_client = ProvisioningDeviceClient.create_from_symmetric_key(
        provisioning_host=provisioning_host,
        registration_id=registration_id,
        id_scope=id_scope,
        symmetric_key=symmetric_key,
    )

    provisioning_device_client.provisioning_payload = {"modelId": model_id}
    return await provisioning_device_client.register()


async def main():
    switch = os.getenv("IOTHUB_DEVICE_SECURITY_TYPE")
    if switch == "DPS":
        provisioning_host = (
            os.getenv("IOTHUB_DEVICE_DPS_ENDPOINT")
            if os.getenv("IOTHUB_DEVICE_DPS_ENDPOINT")
            else "global.azure-devices-provisioning.net"
        )
        id_scope = os.getenv("IOTHUB_DEVICE_DPS_ID_SCOPE")
        registration_id = os.getenv("IOTHUB_DEVICE_DPS_DEVICE_ID")
        symmetric_key = os.getenv("IOTHUB_DEVICE_DPS_DEVICE_KEY")

        registration_result = await provision_device(
            provisioning_host, id_scope, registration_id, symmetric_key, model_id
        )

        if registration_result.status == "assigned":
            print("Device was assigned")
            print(registration_result.registration_state.assigned_hub)
            print(registration_result.registration_state.device_id)
            device_client = IoTHubDeviceClient.create_from_symmetric_key(
                symmetric_key=symmetric_key,
                hostname=registration_result.registration_state.assigned_hub,
                device_id=registration_result.registration_state.device_id,
                product_info=model_id,
            )
        else:
            raise RuntimeError(
                "Could not provision device. Aborting Plug and Play device connection."
            )

    elif switch == "connectionString":
        conn_str = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")
        print("Connecting using Connection String " + conn_str)
        device_client = IoTHubDeviceClient.create_from_connection_string(
            conn_str, product_info=model_id
        )
    else:
        raise RuntimeError(
            "At least one choice needs to be made for complete functioning of this sample."
        )

    # Connect the client.
    await device_client.connect()

    ################################################
    # Update readable properties from various components

    properties_root = pnp_helper.create_reported_properties(serialNumber=serial_number)
    properties_sensor1 = pnp_helper.create_reported_properties(
        sensor_1_component_name, maxVMSSinceLastReboot=98.34
    )
    properties_sensor2 = pnp_helper.create_reported_properties(
        sensor_2_component_name, maxVMSSinceLastReboot=48.92
    )
    properties_device_info = pnp_helper.create_reported_properties(
        device_information_component_name,
        swVersion="5.5",
        manufacturer="Contoso Device Corporation",
        model="Contoso 4762B-turbo",
        osName="Mac Os",
        processorArchitecture="x86-64",
        processorManufacturer="Intel",
        totalStorage=1024,
        totalMemory=32,
    )

    property_updates = asyncio.gather(
        device_client.patch_twin_reported_properties(properties_root),
        device_client.patch_twin_reported_properties(properties_sensor1),
        device_client.patch_twin_reported_properties(properties_sensor2),
        device_client.patch_twin_reported_properties(properties_device_info),
    )

    ################################################
    # Get all the listeners running
    print("Listening for command requests and property updates")

    global SENSOR_1
    global SENSOR_2
    SENSOR_1 = Sensor(sensor_1_component_name, 10)
    SENSOR_2 = Sensor(sensor_2_component_name, 10)

    listeners = asyncio.gather(
        execute_command_listener(
            device_client, method_name="reboot", user_command_handler=reboot_handler
        ),
        execute_command_listener(
            device_client,
            sensor_1_component_name,
            method_name="getMaxMinReport",
            user_command_handler=max_min_handler,
            create_user_response_handler=create_max_min_report_response,
        ),
        execute_command_listener(
            device_client,
            sensor_2_component_name,
            method_name="getMaxMinReport",
            user_command_handler=max_min_handler,
            create_user_response_handler=create_max_min_report_response,
        ),
        execute_property_listener(device_client),
    )

    ################################################
    # Function to send telemetry every 8 seconds

    async def send_telemetry():
        print("Sending telemetry from various components")

        while True:
            curr_vsm_ext = random.randrange(10, 50)
            SENSOR_1.record(curr_vsm_ext)

            moisture_msg1 = {"MoistureValue": curr_vsm_ext}
            await send_telemetry_from_vsm_controller(
                device_client, moisture_msg1, sensor_1_component_name
            )

            curr_vsm_int = random.randrange(10, 50)  # Current moisture in Celsius
            SENSOR_2.record(curr_vsm_int)

            moisture_msg2 = {"MoistureValue": curr_vsm_int}

            await send_telemetry_from_vsm_controller(
                device_client, moisture_msg2, sensor_2_component_name
            )

    send_telemetry_task = asyncio.ensure_future(send_telemetry())

    # Run the stdin listener in the event loop
    loop = asyncio.get_running_loop()
    user_finished = loop.run_in_executor(None, stdin_listener)
    # # Wait for user to indicate they are done listening for method calls
    await user_finished

    if not listeners.done():
        listeners.set_result("DONE")

    if not property_updates.done():
        property_updates.set_result("DONE")

    listeners.cancel()
    property_updates.cancel()

    send_telemetry_task.cancel()

    # Finally, shut down the client
    await device_client.shutdown()


#####################################################
# EXECUTE MAIN

if __name__ == "__main__":
    asyncio.run(main())

    # If using Python 3.6 use the following code instead of asyncio.run(main()):
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
    # loop.close()