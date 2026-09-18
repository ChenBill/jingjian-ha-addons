#!/usr/bin/env python3
"""JingJian 智能开关时序网关。

该模块把 MQTT 命令转换为 Zigbee2MQTT 的设备级 set 命令。目标解析和时序
执行器不依赖 MQTT，便于在 Home Assistant App 外进行单元测试。
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping

try:
    import paho.mqtt.client as mqtt
except ImportError:  # pragma: no cover - 本地单元测试不需要 MQTT 依赖
    mqtt = None


LOGGER = logging.getLogger("jingjian.sequence_gateway")
DEFAULT_COMMAND_TOPIC = "jingjian/smart-switch/sequences/commands"
DEFAULT_STATUS_TOPIC = "jingjian/smart-switch/sequences/status"
DEFAULT_ZIGBEE_BASE_TOPIC = "zigbee2mqtt"
DEFAULT_WAIT_AFTER_MS = 1500
MAX_WAIT_AFTER_MS = 10 * 60 * 1000
SNAPSHOT_DEVICES_TOPIC = "zigbee2mqtt/bridge/devices"
SNAPSHOT_GROUPS_TOPIC = "zigbee2mqtt/bridge/groups"


class CommandError(ValueError):
    """表示客户端命令不能被安全执行的错误。"""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


@dataclass(frozen=True)
class DeviceTarget:
    """一次执行中可发送 Zigbee2MQTT 命令的设备目标。"""

    device_id: str
    friendly_name: str


def normalize_id(value: Any) -> str:
    """统一 IEEE 地址、设备 ID 和分组 ID 的比较格式。"""

    text = str(value or "").strip().lower()
    if text.startswith("0x"):
        text = text[2:]
    return re.sub(r"[^0-9a-z]", "", text)


def _first_value(record: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = record.get(key)
        if value is not None and value != "":
            return value
    return None


def _as_records(snapshot: Any, *container_keys: str) -> list[Mapping[str, Any]]:
    """兼容 Zigbee2MQTT 数组快照和带 data/items 包装的快照。"""

    if isinstance(snapshot, str):
        try:
            snapshot = json.loads(snapshot)
        except json.JSONDecodeError as error:
            raise CommandError("invalid_snapshot", "设备或分组快照不是有效 JSON") from error
    if isinstance(snapshot, list):
        return [item for item in snapshot if isinstance(item, Mapping)]
    if isinstance(snapshot, Mapping):
        for key in container_keys + ("data", "items"):
            value = snapshot.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, Mapping)]
    raise CommandError("invalid_snapshot", "设备或分组快照结构无效")


def _device_id(record: Mapping[str, Any]) -> str:
    value = _first_value(record, "ieee_address", "ieeeAddr", "ieee", "device_id", "id")
    return normalize_id(value)


def _group_id(record: Mapping[str, Any]) -> str:
    value = _first_value(record, "id", "group_id", "groupId")
    return normalize_id(value)


def _member_ids(group: Mapping[str, Any]) -> Iterable[str]:
    members = _first_value(group, "members", "devices", "member")
    if not isinstance(members, list):
        return []
    result: list[str] = []
    for member in members:
        if isinstance(member, Mapping):
            value = _first_value(
                member,
                "ieee_address",
                "ieeeAddr",
                "ieee",
                "device_id",
                "deviceId",
                "device",
                "id",
            )
        else:
            value = member
        member_id = normalize_id(value)
        if member_id:
            result.append(member_id)
    return result


def resolve_targets(
    devices_snapshot: Any,
    groups_snapshot: Any,
    group_ids: Iterable[Any],
    standalone_device_ids: Iterable[Any],
) -> list[DeviceTarget]:
    """按请求顺序展开分组和独立设备，并对目标做稳定去重。"""

    devices = _as_records(devices_snapshot, "devices")
    groups = _as_records(groups_snapshot, "groups")
    device_by_id: dict[str, DeviceTarget] = {}
    for record in devices:
        device_id = _device_id(record)
        friendly_name = _first_value(record, "friendly_name", "friendlyName", "name")
        if not device_id or not isinstance(friendly_name, str) or not friendly_name.strip():
            continue
        if bool(record.get("disabled", False)):
            continue
        device_by_id.setdefault(
            device_id,
            DeviceTarget(device_id=device_id, friendly_name=friendly_name.strip()),
        )

    group_by_id = {_group_id(record): record for record in groups if _group_id(record)}
    requested_group_ids = [normalize_id(value) for value in group_ids if normalize_id(value)]
    requested_device_ids = [
        normalize_id(value) for value in standalone_device_ids if normalize_id(value)
    ]
    selected: list[DeviceTarget] = []
    selected_ids: set[str] = set()

    def add_device(device_id: str) -> None:
        target = device_by_id.get(device_id)
        if target is None:
            raise CommandError("device_not_found", f"设备 {device_id} 不存在、已禁用或未准备好")
        if device_id not in selected_ids:
            selected_ids.add(device_id)
            selected.append(target)

    for group_id in requested_group_ids:
        group = group_by_id.get(group_id)
        if group is None:
            raise CommandError("group_not_found", f"分组 {group_id} 不存在")
        members = list(_member_ids(group))
        if not members:
            raise CommandError("group_empty", f"分组 {group_id} 没有设备")
        for member_id in members:
            add_device(member_id)

    for device_id in requested_device_ids:
        add_device(device_id)

    if not selected:
        raise CommandError("no_valid_targets", "没有可执行的设备或分组")
    return selected


def validate_command(payload: Mapping[str, Any]) -> dict[str, Any]:
    """校验并补全收银台发送的即时执行命令。"""

    if not isinstance(payload, Mapping):
        raise CommandError("invalid_command", "命令必须是 JSON 对象")
    if payload.get("schemaVersion", 1) != 1:
        raise CommandError("unsupported_schema", "不支持的命令协议版本")
    if payload.get("command", "execute_sequence") != "execute_sequence":
        raise CommandError("unsupported_command", "不支持的命令类型")
    request_id = payload.get("requestId")
    if not isinstance(request_id, str) or not request_id.strip():
        raise CommandError("invalid_request_id", "requestId 不能为空")
    action = payload.get("action")
    if action not in ("turn_on", "turn_off"):
        raise CommandError("invalid_action", "action 只能是 turn_on 或 turn_off")

    def read_ids(key: str) -> list[str]:
        values = payload.get(key, [])
        if not isinstance(values, list) or any(not isinstance(value, (str, int)) for value in values):
            raise CommandError("invalid_targets", f"{key} 必须是字符串或数字数组")
        return [str(value) for value in values]

    wait_after_ms = payload.get("waitAfterMs", DEFAULT_WAIT_AFTER_MS)
    if isinstance(wait_after_ms, bool) or not isinstance(wait_after_ms, int):
        raise CommandError("invalid_wait", "waitAfterMs 必须是整数")
    if wait_after_ms < 0 or wait_after_ms > MAX_WAIT_AFTER_MS:
        raise CommandError("invalid_wait", f"waitAfterMs 必须在 0 到 {MAX_WAIT_AFTER_MS} 之间")
    return {
        "schemaVersion": 1,
        "command": "execute_sequence",
        "requestId": request_id.strip(),
        "action": action,
        "groupIds": read_ids("groupIds"),
        "standaloneDeviceIds": read_ids("standaloneDeviceIds"),
        "waitAfterMs": wait_after_ms,
    }


PublishEvent = Callable[[dict[str, Any]], None]
SleepFunction = Callable[[int], None]


class SequenceGateway:
    """管理快照、时序执行锁、请求幂等和 MQTT 业务状态。"""

    def __init__(
        self,
        publish: PublishEvent,
        *,
        command_topic: str = DEFAULT_COMMAND_TOPIC,
        status_topic: str = DEFAULT_STATUS_TOPIC,
        zigbee_base_topic: str = DEFAULT_ZIGBEE_BASE_TOPIC,
        gateway_id: str = "ha-sequence-gateway",
        sleep_fn: SleepFunction | None = None,
    ) -> None:
        self.publish = publish
        self.command_topic = command_topic.rstrip("/")
        self.status_topic = status_topic.rstrip("/")
        self.zigbee_base_topic = zigbee_base_topic.rstrip("/")
        self.gateway_id = gateway_id
        self.sleep_fn = sleep_fn or (lambda milliseconds: time.sleep(milliseconds / 1000))
        self._devices: Any = []
        self._groups: Any = []
        self._devices_ready = False
        self._groups_ready = False
        self._execution_lock = threading.Lock()
        self._results: dict[str, dict[str, Any]] = {}
        self._execution_number = 0

    @property
    def snapshots_ready(self) -> bool:
        """返回设备和分组 retained 快照是否均已到达。"""

        return self._devices_ready and self._groups_ready

    def set_devices(self, snapshot: Any) -> None:
        """更新设备快照并标记设备侧初始化完成。"""

        _as_records(snapshot, "devices")
        self._devices = snapshot
        self._devices_ready = True

    def set_groups(self, snapshot: Any) -> None:
        """更新分组快照并标记分组侧初始化完成。"""

        _as_records(snapshot, "groups")
        self._groups = snapshot
        self._groups_ready = True

    def publish_lifecycle(self, status: str, message: str | None = None) -> dict[str, Any]:
        """发布网关启动或空闲状态。"""

        result: dict[str, Any] = {
            "schemaVersion": 1,
            "gatewayId": self.gateway_id,
            "status": status,
            "updatedAt": _now_iso(),
        }
        if message:
            result["message"] = message
        self._publish_status(result)
        return result

    def handle_command(self, payload: Mapping[str, Any], retained: bool = False) -> dict[str, Any]:
        """校验并执行一次收银台命令，返回可用于日志和测试的状态对象。"""

        request_id = payload.get("requestId") if isinstance(payload, Mapping) else None
        request_id = request_id if isinstance(request_id, str) and request_id.strip() else _new_id("invalid")
        try:
            command = validate_command(payload)
        except CommandError as error:
            return self._reject(request_id, error.reason, error.message)

        request_id = command["requestId"]
        cached = self._results.get(request_id)
        if cached is not None:
            duplicate = copy.deepcopy(cached)
            duplicate["duplicate"] = True
            self._publish_status(duplicate)
            return duplicate
        if retained:
            return self._reject(request_id, "retained_command", "即时命令禁止使用 retained")
        if not self.snapshots_ready:
            return self._reject(request_id, "snapshots_not_ready", "设备和分组快照尚未准备完成")
        if not self._execution_lock.acquire(blocking=False):
            return self._reject(request_id, "already_running", "当前已有时序任务执行中")

        try:
            targets = resolve_targets(
                self._devices,
                self._groups,
                command["groupIds"],
                command["standaloneDeviceIds"],
            )
            execution_id = _new_id("exec")
            base = {
                "schemaVersion": 1,
                "gatewayId": self.gateway_id,
                "requestId": request_id,
                "executionId": execution_id,
                "action": command["action"],
                "targetCount": len(targets),
            }
            self._publish_status({**base, "status": "accepted", "updatedAt": _now_iso()})
            self._publish_status({**base, "status": "running", "updatedAt": _now_iso()})
            state = "ON" if command["action"] == "turn_on" else "OFF"
            for index, target in enumerate(targets):
                self._publish_device_command(target, state)
                if index < len(targets) - 1:
                    self.sleep_fn(command["waitAfterMs"])
            result = {**base, "status": "completed", "updatedAt": _now_iso()}
            self._results[request_id] = copy.deepcopy(result)
            self._publish_status(result)
            return result
        except CommandError as error:
            return self._reject(request_id, error.reason, error.message)
        except Exception as error:  # pragma: no cover - 具体 MQTT 故障由运行环境触发
            LOGGER.exception("时序执行失败")
            result = {
                **base,
                "status": "failed",
                "reason": "execution_failed",
                "message": str(error),
                "updatedAt": _now_iso(),
            }
            self._results[request_id] = copy.deepcopy(result)
            self._publish_status(result)
            return result
        finally:
            self._execution_lock.release()

    def _publish_device_command(self, target: DeviceTarget, state: str) -> None:
        """发布不保留的 Zigbee2MQTT 设备控制命令。"""

        self.publish(
            {
                "kind": "device",
                "topic": f"{self.zigbee_base_topic}/{target.friendly_name}/set",
                "payload": json.dumps({"state": state}, ensure_ascii=False),
                "qos": 1,
                "retain": False,
                "deviceId": target.device_id,
                "friendlyName": target.friendly_name,
            }
        )

    def _publish_status(self, result: Mapping[str, Any]) -> None:
        """发布保留的网关状态，便于收银台重连后恢复状态。"""

        payload = dict(result)
        self.publish(
            {
                "kind": "status",
                "topic": self.status_topic,
                "payload": json.dumps(payload, ensure_ascii=False),
                "qos": 1,
                "retain": True,
                **payload,
            }
        )

    def _reject(self, request_id: str, reason: str, message: str) -> dict[str, Any]:
        result = {
            "schemaVersion": 1,
            "gatewayId": self.gateway_id,
            "requestId": request_id,
            "status": "rejected",
            "reason": reason,
            "message": message,
            "updatedAt": _now_iso(),
        }
        self._publish_status(result)
        return result


class MqttSequenceApplication:
    """Home Assistant App 的 MQTT 适配层和生命周期入口。"""

    def __init__(self, options: Mapping[str, Any]) -> None:
        self.options = options
        self.command_topic = str(options.get("command_topic", DEFAULT_COMMAND_TOPIC)).rstrip("/")
        self.status_topic = str(options.get("status_topic", DEFAULT_STATUS_TOPIC)).rstrip("/")
        self.zigbee_base_topic = str(
            options.get("zigbee_base_topic", DEFAULT_ZIGBEE_BASE_TOPIC)
        ).rstrip("/")
        self.devices_topic = f"{self.zigbee_base_topic}/bridge/devices"
        self.groups_topic = f"{self.zigbee_base_topic}/bridge/groups"
        self.client: Any = None
        self.gateway = SequenceGateway(
            self._publish_event,
            command_topic=self.command_topic,
            status_topic=self.status_topic,
            zigbee_base_topic=self.zigbee_base_topic,
            gateway_id=str(options.get("gateway_id", "ha-sequence-gateway")),
            sleep_fn=lambda milliseconds: time.sleep(milliseconds / 1000),
        )

    def run_forever(self) -> None:
        """连接 MQTT 并在断线后由 Paho 自动重连和重新订阅。"""

        if mqtt is None:
            raise RuntimeError("缺少 paho-mqtt 依赖")
        self.client = _create_mqtt_client(str(self.options.get("client_id", "jingjian-sequence-gateway")))
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        host = str(self.options.get("mqtt_host", "core-mosquitto"))
        port = int(self.options.get("mqtt_port", 1883))
        username = str(self.options.get("mqtt_username", "cashier"))
        password = str(self.options.get("mqtt_password", "cashier"))
        self.client.username_pw_set(username, password)
        self.gateway.publish_lifecycle("starting", "正在等待 Zigbee2MQTT 设备和分组快照")
        self.client.connect(host, port, keepalive=60)
        self.client.loop_forever()

    def _on_connect(self, client: Any, _userdata: Any, _flags: Any, reason_code: Any, *args: Any) -> None:
        if _reason_code_value(reason_code) != 0:
            LOGGER.error("MQTT 连接失败: %s", reason_code)
            return
        for topic in (self.command_topic, self.devices_topic, self.groups_topic):
            client.subscribe(topic, qos=1)
        self.gateway.publish_lifecycle("connected", "MQTT 已连接，等待或刷新 retained 快照")

    def _on_disconnect(self, _client: Any, _userdata: Any, _disconnect_flags: Any, reason_code: Any, *args: Any) -> None:
        LOGGER.warning("MQTT 已断开: %s", reason_code)
        self.gateway.publish_lifecycle("disconnected", "MQTT 连接已断开，等待自动重连")

    def _on_message(self, _client: Any, _userdata: Any, message: Any) -> None:
        try:
            payload = json.loads(message.payload.decode("utf-8"))
            if message.topic == self.devices_topic:
                self.gateway.set_devices(payload)
                self._publish_idle_when_ready()
                return
            if message.topic == self.groups_topic:
                self.gateway.set_groups(payload)
                self._publish_idle_when_ready()
                return
            if message.topic == self.command_topic:
                self.gateway.handle_command(payload, retained=bool(message.retain))
        except (UnicodeDecodeError, json.JSONDecodeError, CommandError) as error:
            LOGGER.error("MQTT 消息处理失败: %s", error)

    def _publish_idle_when_ready(self) -> None:
        if self.gateway.snapshots_ready:
            self.gateway.publish_lifecycle("idle", "设备和分组快照已准备完成")

    def _publish_event(self, event: dict[str, Any]) -> None:
        if self.client is None:
            return
        self.client.publish(
            event["topic"],
            event["payload"],
            qos=int(event.get("qos", 1)),
            retain=bool(event.get("retain", False)),
        )


def _create_mqtt_client(client_id: str) -> Any:
    """兼容 paho-mqtt 1.x 和 2.x 的客户端构造方式。"""

    if mqtt is None:  # pragma: no cover
        raise RuntimeError("缺少 paho-mqtt 依赖")
    callback_version = getattr(getattr(mqtt, "CallbackAPIVersion", None), "VERSION2", None)
    if callback_version is not None:
        return mqtt.Client(callback_api_version=callback_version, client_id=client_id)
    return mqtt.Client(client_id=client_id)


def _reason_code_value(reason_code: Any) -> int:
    try:
        return int(reason_code)
    except (TypeError, ValueError):
        return 0 if str(reason_code).lower() in ("success", "0") else 1


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def load_options(path: str = "/data/options.json") -> dict[str, Any]:
    """读取 Home Assistant App 注入的 options.json。"""

    with open(path, "r", encoding="utf-8") as file:
        options = json.load(file)
    if not isinstance(options, dict):
        raise ValueError("/data/options.json 必须是 JSON 对象")
    return options


def main() -> None:
    """应用进程入口。"""

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    MqttSequenceApplication(load_options()).run_forever()


if __name__ == "__main__":
    main()
