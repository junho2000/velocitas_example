# Copyright (c) 2022 Robert Bosch GmbH and Microsoft Corporation
#
# This program and the accompanying materials are made available under the
# terms of the Apache License, Version 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# SPDX-License-Identifier: Apache-2.0

"""A sample skeleton vehicle app."""

import asyncio
import json
import logging
import signal
from typing import Deque, Tuple
from collections import deque
from datetime import datetime, timedelta
from vehicle import Vehicle, vehicle  # type: ignore
from velocitas_sdk.util.log import (  # type: ignore
    get_opentelemetry_log_factory,
    get_opentelemetry_log_format,
)
from velocitas_sdk.vdb.reply import DataPointReply
from velocitas_sdk.vehicle_app import VehicleApp, subscribe_topic

# Configure the VehicleApp logger with the necessary log config and level.
logging.setLogRecordFactory(get_opentelemetry_log_factory())
logging.basicConfig(format=get_opentelemetry_log_format())
logging.getLogger().setLevel("DEBUG")
logger = logging.getLogger(__name__)

GET_SPEED_REQUEST_TOPIC = "sampleapp/getSpeed"
GET_SPEED_RESPONSE_TOPIC = "sampleapp/getSpeed/response"
DATABROKER_SUBSCRIPTION_TOPIC = "sampleapp/currentSpeed"


acceleration_data: Deque[Tuple[datetime, float]] = deque(maxlen=5000)

ACCELERATION_THRESHOLD = 50


def calculate_acceleration(v_initial, v_final, time):
    if v_initial is None:
        return 0
    if time <= 0:
        raise ValueError("Time interval must be greater than zero.")
    acceleration = (v_final - v_initial) / time
    # print(acceleration)
    return acceleration


def check_for_anomalies(acceleration_value: float) -> bool:
    """Check if the acceleration value indicates an anomaly (e.g., car accident)."""
    return abs(acceleration_value) > ACCELERATION_THRESHOLD


class SampleApp(VehicleApp):
    """
    Sample skeleton vehicle app.

    The skeleton subscribes to a getSpeed MQTT topic
    to listen for incoming requests to get
    the current vehicle speed and publishes it to
    a response topic.

    It also subcribes to the VehicleDataBroker
    directly for updates of the
    Vehicle.Speed signal and publishes this
    information via another specific MQTT topic
    """

    def __init__(self, vehicle_client: Vehicle):
        # SampleApp inherits from VehicleApp.
        super().__init__()
        self.Vehicle = vehicle_client
        self.previous_time = datetime.now().timestamp()
        self.previous_speed = 0.0

    async def on_start(self):
        """Run when the vehicle app starts"""
        # This method will be called by the SDK when the connection to the
        # Vehicle DataBroker is ready.
        # Here you can subscribe for the Vehicle Signals update (e.g. Vehicle Speed).
        await self.Vehicle.Speed.subscribe(self.on_speed_change)

    async def on_speed_change(self, data: DataPointReply):
        """The on_speed_change callback, this will be executed when receiving a new
        vehicle signal updates."""
        # Get the current vehicle speed value from the received DatapointReply.
        # The DatapointReply containes the values of all subscribed DataPoints of
        # the same callback.
        vehicle_speed = data.get(self.Vehicle.Speed).value
        current_time = datetime.now().timestamp()

        if self.previous_time:
            time_diff = current_time - self.previous_time
            acceleration = calculate_acceleration(
                self.previous_speed, vehicle_speed, time_diff
            )
            # Add the new acceleration data to the deque
            acceleration_data.append((datetime.now(), acceleration))
            # print("Append!")
            # Remove data older than 5 minutes
            five_minutes_ago = datetime.now() - timedelta(minutes=5)
            while acceleration_data and acceleration_data[0][0] < five_minutes_ago:
                acceleration_data.popleft()
                # print("Pop!")
            if check_for_anomalies(acceleration):
                print(f"Anomaly detected at {current_time}: Car accident may have \
                    happened.")
        # Do anything with the received value.
        # Example:
        # - Publishes current speed to MQTT Topic (i.e. DATABROKER_SUBSCRIPTION_TOPIC).
        await self.publish_event(
            DATABROKER_SUBSCRIPTION_TOPIC,
            json.dumps({"speed": vehicle_speed}),
        )
        self.previous_time = current_time
        self.previous_speed = vehicle_speed

    @subscribe_topic(GET_SPEED_REQUEST_TOPIC)
    async def on_get_speed_request_received(self, data: str) -> None:
        """The subscribe_topic annotation is used to subscribe for incoming
        PubSub events, e.g. MQTT event for GET_SPEED_REQUEST_TOPIC.
        """

        # Use the logger with the preferred log level (e.g. debug, info, error, etc)
        logger.debug(
            "PubSub event for the Topic: %s -> is received with the data: %s",
            GET_SPEED_REQUEST_TOPIC,
            data,
        )

        # Getting current speed from VehicleDataBroker using the DataPoint getter.
        vehicle_speed = (await self.Vehicle.Speed.get()).value

        # Do anything with the speed value.
        # Example:
        # - Publishes the vehicle speed to MQTT topic (i.e. GET_SPEED_RESPONSE_TOPIC).
        await self.publish_event(
            GET_SPEED_RESPONSE_TOPIC,
            json.dumps(
                {
                    "result": {
                        "status": 0,
                        "message": f"""Current Speed = {vehicle_speed}""",
                    },
                }
            ),
        )


async def main():
    """Main function"""
    logger.info("Starting SampleApp...")
    # Constructing SampleApp and running it.
    vehicle_app = SampleApp(vehicle)
    await vehicle_app.run()


LOOP = asyncio.get_event_loop()
LOOP.add_signal_handler(signal.SIGTERM, LOOP.stop)
LOOP.run_until_complete(main())
LOOP.close()
