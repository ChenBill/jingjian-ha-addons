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
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    import paho.mqtt.client as mqtt
except ImportError:  # pragma: no cover - 本地单元测试不需要 MQTT 依赖
    mqtt = None


LOGGER = logging.getLogger("jingjian.sequence_gateway")
DEFAULT_COMMAND_TOPIC = "jingjian/smart-switch/sequences/commands"
DEFAULT_STATUS_TOPIC = "jingjian/smart-switch/sequences/status"
DEFAULT_SCHEDULE_TOPIC = "jingjian/smart-switch/schedules"
DEFAULT_SCHEDULE_STATUS_TOPIC = "jingjian/smart-switch/schedules/status"
DEFAULT_TIME_ZONE = "Asia/Shanghai"
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


def validate_schedule(payload: Mapping[str, Any]) -> dict[str, Any]:
    """校验并规范收银台发布的 retained 定时方案。"""

    if not isinstance(payload, Mapping):
        raise CommandError("invalid_schedule", "定时方案必须是 JSON 对象")
    schedule_id = payload.get("id")
    if not isinstance(schedule_id, str) or not schedule_id.strip():
        raise CommandError("invalid_schedule_id", "定时方案 id 不能为空")
    if payload.get("schemaVersion", 1) != 1:
        raise CommandError("unsupported_schema", "不支持的定时方案协议版本")
    revision = payload.get("revision", 1)
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise CommandError("invalid_revision", "定时方案 revision 必须是正整数")
    deleted = payload.get("deleted", False)
    if not isinstance(deleted, bool):
        raise CommandError("invalid_schedule", "deleted 必须是布尔值")
    if deleted:
        return {"schemaVersion": 1, "id": schedule_id.strip(), "revision": revision, "deleted": True}

    time_zone = payload.get("timeZone", DEFAULT_TIME_ZONE)
    if not isinstance(time_zone, str) or not time_zone.strip():
        raise CommandError("invalid_timezone", "定时方案时区不能为空")
    try:
        _resolve_time_zone(time_zone.strip())
    except ZoneInfoNotFoundError as error:
        raise CommandError("invalid_timezone", f"不支持的定时方案时区：{time_zone}") from error

    raw_events = payload.get("events", [])
    if not isinstance(raw_events, list):
        raise CommandError("invalid_events", "定时方案 events 必须是数组")
    events: list[dict[str, Any]] = []
    for raw_event in raw_events:
        if not isinstance(raw_event, Mapping):
            raise CommandError("invalid_event", "定时方案事件结构无效")
        event_id = raw_event.get("id")
        event_time = raw_event.get("time")
        weekdays = raw_event.get("weekdays")
        commands = raw_event.get("commands")
        if not isinstance(event_id, str) or not event_id.strip():
            raise CommandError("invalid_event", "定时方案事件 id 不能为空")
        if not isinstance(event_time, str) or not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", event_time):
            raise CommandError("invalid_event_time", f"事件 {event_id} 的时间必须是 HH:mm")
        if not isinstance(weekdays, list) or not weekdays or any(day not in ("mon", "tue", "wed", "thu", "fri", "sat", "sun") for day in weekdays):
            raise CommandError("invalid_weekdays", f"事件 {event_id} 的 weekdays 无效")
        if not isinstance(commands, list) or not commands:
            raise CommandError("invalid_commands", f"事件 {event_id} 没有可执行目标")
        normalized_commands: list[dict[str, Any]] = []
        for command in commands:
            if not isinstance(command, Mapping):
                raise CommandError("invalid_command", f"事件 {event_id} 的目标结构无效")
            ieee = normalize_id(command.get("ieee") or command.get("deviceId"))
            payload_value = command.get("payload")
            state = payload_value.get("state") if isinstance(payload_value, Mapping) else None
            if not ieee or state not in ("ON", "OFF"):
                raise CommandError("invalid_command", f"事件 {event_id} 的目标缺少稳定设备 ID 或 state")
            normalized_commands.append({
                "ieee": ieee,
                "deviceId": normalize_id(command.get("deviceId") or ieee),
                "payload": {"state": state},
            })
        events.append({
            "id": event_id.strip(),
            "time": event_time,
            "weekdays": list(dict.fromkeys(weekdays)),
            "commands": normalized_commands,
        })

    return {
        "schemaVersion": 1,
        "id": schedule_id.strip(),
        "revision": revision,
        "timeZone": time_zone.strip(),
        "deleted": False,
        "events": events,
    }


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
        schedule_status_topic: str = DEFAULT_SCHEDULE_STATUS_TOPIC,
        zigbee_base_topic: str = DEFAULT_ZIGBEE_BASE_TOPIC,
        gateway_id: str = "ha-sequence-gateway",
        sleep_fn: SleepFunction | None = None,
    ) -> None:
        self.publish = publish
        self.command_topic = command_topic.rstrip("/")
        self.status_topic = status_topic.rstrip("/")
        self.schedule_status_topic = schedule_status_topic.rstrip("/")
        self.zigbee_base_topic = zigbee_base_topic.rstrip("/")
        self.gateway_id = gateway_id
        self.sleep_fn = sleep_fn or (lambda milliseconds: time.sleep(milliseconds / 1000))
        self._devices: Any = []
        self._groups: Any = []
        self._devices_ready = False
        self._groups_ready = False
        self._execution_lock = threading.Lock()
        self._results: dict[str, dict[str, Any]] = {}
        self._schedules: dict[str, dict[str, Any]] = {}
        self._schedule_fired_keys: set[str] = set()

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
        try:
            targets = resolve_targets(
                self._devices,
                self._groups,
                command["groupIds"],
                command["standaloneDeviceIds"],
            )
            return self._execute_targets(
                request_id,
                command["action"],
                targets,
                command["waitAfterMs"],
                status_topic=self.status_topic,
                extra={},
                blocking=False,
            )
        except CommandError as error:
            return self._reject(request_id, error.reason, error.message)

    def handle_schedule(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """接收 retained 定时方案，按 revision 更新或删除内存方案。"""

        schedule = validate_schedule(payload)
        schedule_id = schedule["id"]
        current = self._schedules.get(schedule_id)
        if schedule["deleted"] and current is not None and "revision" not in payload:
            schedule["revision"] = current["revision"] + 1
        if current is not None and schedule["revision"] <= current["revision"]:
            result = {
                "schemaVersion": 1,
                "gatewayId": self.gateway_id,
                "scheduleId": schedule_id,
                "revision": current["revision"],
                "status": "ignored",
                "reason": "older_revision",
                "updatedAt": _now_iso(),
            }
            self._publish_status(result, self.schedule_status_topic)
            return result
        if schedule["deleted"]:
            if current is None:
                result = {"schemaVersion": 1, "gatewayId": self.gateway_id, "scheduleId": schedule_id, "status": "ignored", "reason": "missing", "updatedAt": _now_iso()}
            else:
                self._schedules.pop(schedule_id, None)
                result = {"schemaVersion": 1, "gatewayId": self.gateway_id, "scheduleId": schedule_id, "revision": schedule["revision"], "status": "deleted", "updatedAt": _now_iso()}
            self._publish_status(result, self.schedule_status_topic)
            return result
        self._schedules[schedule_id] = schedule
        result = {
            "schemaVersion": 1,
            "gatewayId": self.gateway_id,
            "scheduleId": schedule_id,
            "revision": schedule["revision"],
            "eventCount": len(schedule["events"]),
            "status": "updated",
            "updatedAt": _now_iso(),
        }
        self._publish_status(result, self.schedule_status_topic)
        return result

    def run_schedule_tick(self, now: datetime | None = None) -> int:
        """扫描当前分钟的定时事件并按顺序执行，返回本次触发数量。"""

        current_time = now or datetime.now(timezone.utc)
        triggered = 0
        for schedule in list(self._schedules.values()):
            local_now = current_time.astimezone(_resolve_time_zone(schedule["timeZone"]))
            weekday = local_now.strftime("%a").lower()[:3]
            minute = local_now.strftime("%H:%M")
            for event in schedule["events"]:
                if event["time"] != minute or weekday not in event["weekdays"]:
                    continue
                execution_key = f"{schedule['id']}:{schedule['revision']}:{event['id']}:{local_now:%Y-%m-%d-%H-%M}"
                if execution_key in self._schedule_fired_keys:
                    continue
                self._schedule_fired_keys.add(execution_key)
                triggered += 1
                self._execute_schedule_event(schedule, event, execution_key)
        return triggered

    def _execute_schedule_event(self, schedule: Mapping[str, Any], event: Mapping[str, Any], execution_key: str) -> None:
        """按最新设备快照解析定时事件并复用顺序执行器。"""

        states = {command["payload"]["state"] for command in event["commands"]}
        extra = {
            "scheduleId": schedule["id"],
            "revision": schedule["revision"],
            "eventId": event["id"],
            "executionKey": execution_key,
        }
        if len(states) != 1:
            self._publish_status({"schemaVersion": 1, "gatewayId": self.gateway_id, **extra, "status": "failed", "reason": "mixed_actions", "updatedAt": _now_iso()}, self.schedule_status_topic)
            return
        try:
            targets = self._resolve_schedule_targets(event["commands"])
            action = "turn_on" if next(iter(states)) == "ON" else "turn_off"
            self._execute_targets(
                execution_key,
                action,
                targets,
                DEFAULT_WAIT_AFTER_MS,
                status_topic=self.schedule_status_topic,
                extra=extra,
                blocking=True,
            )
        except CommandError as error:
            self._publish_status({"schemaVersion": 1, "gatewayId": self.gateway_id, **extra, "status": "failed", "reason": error.reason, "message": error.message, "updatedAt": _now_iso()}, self.schedule_status_topic)

    def _resolve_schedule_targets(self, commands: Iterable[Mapping[str, Any]]) -> list[DeviceTarget]:
        """根据稳定 IEEE 地址从最新 Zigbee2MQTT 快照解析设备名称。"""

        devices = _as_records(self._devices, "devices")
        by_id: dict[str, DeviceTarget] = {}
        for record in devices:
            device_id = _device_id(record)
            friendly_name = _first_value(record, "friendly_name", "friendlyName", "name")
            if device_id and isinstance(friendly_name, str) and friendly_name.strip() and not bool(record.get("disabled", False)):
                by_id[device_id] = DeviceTarget(device_id=device_id, friendly_name=friendly_name.strip())
        selected: list[DeviceTarget] = []
        seen: set[str] = set()
        for command in commands:
            device_id = normalize_id(command.get("ieee") or command.get("deviceId"))
            target = by_id.get(device_id)
            if target is None:
                raise CommandError("device_not_found", f"设备 {device_id} 不存在、已禁用或未准备好")
            if device_id not in seen:
                seen.add(device_id)
                selected.append(target)
        if not selected:
            raise CommandError("no_valid_targets", "定时事件没有可执行设备")
        return selected

    def _execute_targets(
        self,
        request_id: str,
        action: str,
        targets: list[DeviceTarget],
        wait_after_ms: int,
        *,
        status_topic: str,
        extra: Mapping[str, Any],
        blocking: bool,
    ) -> dict[str, Any]:
        """执行已解析目标并统一发布即时或定时状态。"""

        if not self._execution_lock.acquire(blocking=blocking):
            return self._reject(request_id, "already_running", "当前已有时序任务执行中", status_topic=status_topic, extra=extra)
        try:
            base = {"schemaVersion": 1, "gatewayId": self.gateway_id, "requestId": request_id, "executionId": _new_id("exec"), "action": action, "targetCount": len(targets), **extra}
            self._publish_status({**base, "status": "accepted", "updatedAt": _now_iso()}, status_topic)
            self._publish_status({**base, "status": "running", "updatedAt": _now_iso()}, status_topic)
            state = "ON" if action == "turn_on" else "OFF"
            for index, target in enumerate(targets):
                self._publish_device_command(target, state)
                if index < len(targets) - 1:
                    self.sleep_fn(wait_after_ms)
            result = {**base, "status": "completed", "updatedAt": _now_iso()}
            self._results[request_id] = copy.deepcopy(result)
            self._publish_status(result, status_topic)
            return result
        except Exception as error:  # pragma: no cover - 具体 MQTT 故障由运行环境触发
            LOGGER.exception("时序执行失败")
            result = {**base, "status": "failed", "reason": "execution_failed", "message": str(error), "updatedAt": _now_iso()}
            self._results[request_id] = copy.deepcopy(result)
            self._publish_status(result, status_topic)
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

    def _publish_status(self, result: Mapping[str, Any], topic: str | None = None) -> None:
        """发布保留的网关状态，便于收银台重连后恢复状态。"""

        payload = dict(result)
        self.publish(
            {
                "kind": "status",
                "topic": topic or self.status_topic,
                "payload": json.dumps(payload, ensure_ascii=False),
                "qos": 1,
                "retain": True,
                **payload,
            }
        )

    def _reject(
        self,
        request_id: str,
        reason: str,
        message: str,
        *,
        status_topic: str | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        result = {
            "schemaVersion": 1,
            "gatewayId": self.gateway_id,
            "requestId": request_id,
            "status": "rejected",
            "reason": reason,
            "message": message,
            "updatedAt": _now_iso(),
            **(extra or {}),
        }
        self._publish_status(result, status_topic)
        return result


class MqttSequenceApplication:
    """Home Assistant App 的 MQTT 适配层和生命周期入口。"""

    def __init__(self, options: Mapping[str, Any]) -> None:
        self.options = options
        self.command_topic = str(options.get("command_topic", DEFAULT_COMMAND_TOPIC)).rstrip("/")
        self.status_topic = str(options.get("status_topic", DEFAULT_STATUS_TOPIC)).rstrip("/")
        self.schedule_topic = str(options.get("schedule_topic", DEFAULT_SCHEDULE_TOPIC)).rstrip("/")
        self.schedule_status_topic = str(options.get("schedule_status_topic", DEFAULT_SCHEDULE_STATUS_TOPIC)).rstrip("/")
        self.schedule_time_zone = str(options.get("default_time_zone", DEFAULT_TIME_ZONE)).strip() or DEFAULT_TIME_ZONE
        self.schedule_tick_seconds = max(1, int(options.get("schedule_tick_seconds", 1)))
        self.zigbee_base_topic = str(
            options.get("zigbee_base_topic", DEFAULT_ZIGBEE_BASE_TOPIC)
        ).rstrip("/")
        self.devices_topic = f"{self.zigbee_base_topic}/bridge/devices"
        self.groups_topic = f"{self.zigbee_base_topic}/bridge/groups"
        self.client: Any = None
        self._connected = False
        self._schedule_stop = threading.Event()
        self._schedule_thread: threading.Thread | None = None
        self.gateway = SequenceGateway(
            self._publish_event,
            command_topic=self.command_topic,
            status_topic=self.status_topic,
            schedule_status_topic=self.schedule_status_topic,
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
        self._start_schedule_thread()
        self.client.loop_forever()

    def _on_connect(self, client: Any, _userdata: Any, _flags: Any, reason_code: Any, *args: Any) -> None:
        if _reason_code_value(reason_code) != 0:
            LOGGER.error("MQTT 连接失败: %s", reason_code)
            return
        self._connected = True
        for topic in (self.command_topic, self.devices_topic, self.groups_topic, f"{self.schedule_topic}/+"):
            client.subscribe(topic, qos=1)
        self.gateway.publish_lifecycle("connected", "MQTT 已连接，等待或刷新 retained 快照")

    def _on_disconnect(self, _client: Any, _userdata: Any, _disconnect_flags: Any, reason_code: Any, *args: Any) -> None:
        self._connected = False
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
                return
            if message.topic == self.schedule_status_topic:
                return
            if message.topic.startswith(f"{self.schedule_topic}/"):
                self.gateway.handle_schedule(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, CommandError) as error:
            LOGGER.error("MQTT 消息处理失败: %s", error)

    def _start_schedule_thread(self) -> None:
        """启动单一后台调度线程，避免多个连接回调重复创建。"""

        if self._schedule_thread is not None and self._schedule_thread.is_alive():
            return
        self._schedule_stop.clear()
        self._schedule_thread = threading.Thread(target=self._run_schedule_loop, name="schedule-loop", daemon=True)
        self._schedule_thread.start()

    def _run_schedule_loop(self) -> None:
        """按秒扫描定时方案，到点后复用顺序执行器。"""

        while not self._schedule_stop.wait(self.schedule_tick_seconds):
            if not self._connected:
                continue
            try:
                self.gateway.run_schedule_tick()
            except Exception:
                LOGGER.exception("定时方案扫描失败")

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


def _resolve_time_zone(name: str):
    """解析时区；系统缺少 tzdata 时兼容上海固定 UTC+8。"""

    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        if name == "Asia/Shanghai":
            return timezone(timedelta(hours=8), name="Asia/Shanghai")
        raise


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
