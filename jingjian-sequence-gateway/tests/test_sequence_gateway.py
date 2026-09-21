import json
import sys
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_DIR))

from sequence_gateway import (  # noqa: E402
    DEFAULT_WAIT_AFTER_MS,
    MqttSequenceApplication,
    SequenceGateway,
    normalize_id,
    resolve_targets,
    validate_schedule,
)


class SequenceGatewayTest(unittest.TestCase):
    def setUp(self) -> None:
        self.devices = [
            {
                "ieee_address": "0x0A:BB:CC:DD:EE:FF:00:01",
                "friendly_name": "front-lamp",
                "disabled": False,
            },
            {
                "ieee_address": "0x0A:BB:CC:DD:EE:FF:00:02",
                "friendly_name": "back-lamp",
                "disabled": False,
            },
            {
                "ieee_address": "0x0A:BB:CC:DD:EE:FF:00:03",
                "friendly_name": "disabled-lamp",
                "disabled": True,
            },
        ]
        self.groups = [
            {
                "id": "1",
                "friendly_name": "front-group",
                "members": [
                    {"ieee_address": "0x0A:BB:CC:DD:EE:FF:00:01"},
                    {"ieee_address": "0x0A:BB:CC:DD:EE:FF:00:02"},
                ],
            }
        ]

    def test_normalize_id_removes_prefix_and_separators(self) -> None:
        self.assertEqual(
            normalize_id(" 0x0A:BB-CC_dd "),
            "0abbccdd",
        )

    def test_resolve_targets_expands_group_and_deduplicates_standalone(self) -> None:
        targets = resolve_targets(
            self.devices,
            self.groups,
            group_ids=["1"],
            standalone_device_ids=["0a:bb:cc:dd:ee:ff:00:02"],
        )

        self.assertEqual(
            [(target.device_id, target.friendly_name) for target in targets],
            [
                ("0abbccddeeff0001", "front-lamp"),
                ("0abbccddeeff0002", "back-lamp"),
            ],
        )

    def test_resolve_targets_accepts_zigbee2mqtt_group_device_member_field(self) -> None:
        targets = resolve_targets(
            self.devices,
            [{"id": 1, "members": [{"device": "0x0A:BB:CC:DD:EE:FF:00:01"}]}],
            group_ids=[1],
            standalone_device_ids=[],
        )

        self.assertEqual([target.friendly_name for target in targets], ["front-lamp"])

    def test_gateway_rejects_command_until_retained_snapshots_are_ready(self) -> None:
        published = []
        gateway = SequenceGateway(publish=published.append)

        result = gateway.handle_command(
            {"requestId": "req-not-ready", "action": "turn_on"},
        )

        self.assertEqual(result["status"], "rejected")
        self.assertEqual(result["reason"], "snapshots_not_ready")
        self.assertEqual(published[-1]["status"], "rejected")

    def test_gateway_publishes_device_commands_in_order_with_default_1500ms_wait(self) -> None:
        published = []
        sleeps = []
        gateway = SequenceGateway(
            publish=published.append,
            sleep_fn=sleeps.append,
        )
        gateway.set_devices(self.devices)
        gateway.set_groups(self.groups)

        result = gateway.handle_command(
            {
                "requestId": "req-on",
                "action": "turn_on",
                "groupIds": ["1"],
                "standaloneDeviceIds": [],
            },
        )

        device_messages = [item for item in published if item["kind"] == "device"]
        self.assertEqual(result["status"], "completed")
        self.assertEqual(
            [item["topic"] for item in device_messages],
            [
                "zigbee2mqtt/front-lamp/set",
                "zigbee2mqtt/back-lamp/set",
            ],
        )
        self.assertEqual(
            [json.loads(item["payload"]) for item in device_messages],
            [{"state": "ON"}, {"state": "ON"}],
        )
        self.assertEqual(sleeps, [DEFAULT_WAIT_AFTER_MS])
        self.assertTrue(all(item["retain"] is False for item in device_messages))

    def test_gateway_does_not_execute_duplicate_request_id(self) -> None:
        published = []
        gateway = SequenceGateway(publish=published.append)
        gateway.set_devices(self.devices)
        gateway.set_groups(self.groups)
        command = {
            "requestId": "req-idempotent",
            "action": "turn_off",
            "groupIds": [],
            "standaloneDeviceIds": ["0a:bb:cc:dd:ee:ff:00:01"],
        }

        first = gateway.handle_command(command)
        second = gateway.handle_command(command)

        device_messages = [item for item in published if item["kind"] == "device"]
        self.assertEqual(first["status"], "completed")
        self.assertEqual(second["status"], "completed")
        self.assertTrue(second["duplicate"])
        self.assertEqual(len(device_messages), 1)

    def test_gateway_rejects_retained_command(self) -> None:
        published = []
        gateway = SequenceGateway(publish=published.append)
        gateway.set_devices(self.devices)
        gateway.set_groups(self.groups)

        result = gateway.handle_command(
            {
                "requestId": "req-retained",
                "action": "turn_on",
                "groupIds": ["1"],
            },
            retained=True,
        )

        self.assertEqual(result["status"], "rejected")
        self.assertEqual(result["reason"], "retained_command")
        self.assertFalse(any(item["kind"] == "device" for item in published))

    def test_gateway_rejects_invalid_wait_and_unknown_group(self) -> None:
        published = []
        gateway = SequenceGateway(publish=published.append)
        gateway.set_devices(self.devices)
        gateway.set_groups(self.groups)

        invalid_wait = gateway.handle_command(
            {
                "requestId": "req-invalid-wait",
                "action": "turn_on",
                "groupIds": ["1"],
                "waitAfterMs": -1,
            }
        )
        unknown_group = gateway.handle_command(
            {
                "requestId": "req-unknown-group",
                "action": "turn_on",
                "groupIds": ["404"],
            }
        )

        self.assertEqual(invalid_wait["reason"], "invalid_wait")
        self.assertEqual(unknown_group["reason"], "group_not_found")
        self.assertFalse(any(item["kind"] == "device" for item in published))

    def test_gateway_marks_publish_failure_and_does_not_replay_failed_request(self) -> None:
        published = []

        def publish(event: dict) -> None:
            published.append(event)
            if event["kind"] == "device":
                raise RuntimeError("broker publish failed")

        gateway = SequenceGateway(publish=publish)
        gateway.set_devices(self.devices)
        gateway.set_groups(self.groups)
        command = {
            "requestId": "req-failed",
            "action": "turn_on",
            "groupIds": ["1"],
        }

        with self.assertLogs("jingjian.sequence_gateway", level="ERROR"):
            first = gateway.handle_command(command)
        second = gateway.handle_command(command)

        self.assertEqual(first["status"], "failed")
        self.assertEqual(second["status"], "failed")
        self.assertTrue(second["duplicate"])
        self.assertEqual(len([item for item in published if item["kind"] == "device"]), 1)

    def test_gateway_allows_only_one_execution_at_a_time(self) -> None:
        published = []
        entered_sleep = threading.Event()
        release_sleep = threading.Event()

        def blocking_sleep(milliseconds: int) -> None:
            entered_sleep.set()
            release_sleep.wait(timeout=2)

        gateway = SequenceGateway(
            publish=published.append,
            sleep_fn=blocking_sleep,
        )
        gateway.set_devices(self.devices)
        gateway.set_groups(self.groups)
        first_result = []

        worker = threading.Thread(
            target=lambda: first_result.append(
                gateway.handle_command(
                    {
                        "requestId": "req-first",
                        "action": "turn_on",
                        "groupIds": ["1"],
                    }
                )
            )
        )
        worker.start()
        self.assertTrue(entered_sleep.wait(timeout=1))

        second = gateway.handle_command(
            {
                "requestId": "req-second",
                "action": "turn_off",
                "groupIds": ["1"],
            }
        )

        release_sleep.set()
        worker.join(timeout=2)
        self.assertEqual(second["status"], "rejected")
        self.assertEqual(second["reason"], "already_running")
        self.assertEqual(first_result[0]["status"], "completed")

    def test_schedule_validation_normalizes_retained_plan_events(self) -> None:
        result = validate_schedule({
            "schemaVersion": 1,
            "id": "plan-1",
            "revision": 2,
            "timeZone": "Asia/Shanghai",
            "events": [{
                "id": "period-1__on",
                "time": "09:00",
                "weekdays": ["mon"],
                "commands": [{
                    "deviceId": "0x0A:BB:CC:DD:EE:FF:00:01",
                    "ieee": "0x0A:BB:CC:DD:EE:FF:00:01",
                    "payload": {"state": "ON"},
                }],
            }],
        })

        self.assertEqual(result["id"], "plan-1")
        self.assertEqual(result["revision"], 2)
        self.assertEqual(result["events"][0]["weekdays"], ["mon"])
        self.assertEqual(result["events"][0]["commands"][0]["ieee"], "0abbccddeeff0001")

    def test_schedule_event_executes_once_and_resolves_latest_device_name(self) -> None:
        published = []
        gateway = SequenceGateway(
            publish=published.append,
            sleep_fn=lambda _milliseconds: None,
            schedule_status_topic="jingjian/smart-switch/schedules/status",
        )
        gateway.set_devices(self.devices)
        gateway.set_groups(self.groups)
        gateway.handle_schedule({
            "schemaVersion": 1,
            "id": "plan-1",
            "revision": 1,
            "timeZone": "Asia/Shanghai",
            "events": [{
                "id": "period-1__on",
                "time": "09:00",
                "weekdays": ["mon"],
                "commands": [{
                    "deviceId": "0a:bb:cc:dd:ee:ff:00:01",
                    "ieee": "0a:bb:cc:dd:ee:ff:00:01",
                    "payload": {"state": "ON"},
                }],
            }],
        })
        renamed_devices = [dict(self.devices[0], friendly_name="new-front-lamp"), *self.devices[1:]]
        gateway.set_devices(renamed_devices)

        shanghai = timezone(timedelta(hours=8), name="Asia/Shanghai")
        first = gateway.run_schedule_tick(datetime(2026, 9, 21, 9, 0, tzinfo=shanghai))
        second = gateway.run_schedule_tick(datetime(2026, 9, 21, 9, 0, 30, tzinfo=shanghai))

        device_messages = [item for item in published if item["kind"] == "device"]
        self.assertEqual(first, 1)
        self.assertEqual(second, 0)
        self.assertEqual(device_messages[0]["topic"], "zigbee2mqtt/new-front-lamp/set")
        self.assertEqual(device_messages[0]["payload"], '{"state": "ON"}')
        self.assertTrue(any(item.get("status") == "completed" and item.get("scheduleId") == "plan-1" for item in published))

    def test_schedule_revision_and_delete_are_idempotent(self) -> None:
        published = []
        gateway = SequenceGateway(publish=published.append)
        gateway.set_devices(self.devices)
        gateway.set_groups(self.groups)
        schedule = {
            "schemaVersion": 1,
            "id": "plan-1",
            "revision": 2,
            "timeZone": "Asia/Shanghai",
            "events": [],
        }

        self.assertEqual(gateway.handle_schedule(schedule)["status"], "updated")
        self.assertEqual(gateway.handle_schedule({**schedule, "revision": 1})["status"], "ignored")
        self.assertEqual(gateway.handle_schedule({"schemaVersion": 1, "id": "plan-1", "deleted": True})["status"], "deleted")
        self.assertEqual(gateway.handle_schedule({"schemaVersion": 1, "id": "plan-1", "deleted": True})["status"], "ignored")

    def test_mqtt_application_subscribes_to_retained_schedule_children(self) -> None:
        app = MqttSequenceApplication({
            "schedule_topic": "jingjian/smart-switch/schedules",
            "schedule_status_topic": "jingjian/smart-switch/schedules/status",
        })
        subscriptions = []

        class FakeClient:
            def subscribe(self, topic, qos):
                subscriptions.append((topic, qos))

        app._on_connect(FakeClient(), None, None, 0)

        self.assertIn(("jingjian/smart-switch/schedules/+", 1), subscriptions)
        app._connected = False


if __name__ == "__main__":
    unittest.main()
