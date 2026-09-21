# JingJian Smart Switch Sequence Gateway

这是一个可通过 Home Assistant App（Add-on）安装的 MQTT 时序执行网关。收银台只发送分组 ID、独立设备 ID 和动作，网关在执行开始时读取 Zigbee2MQTT 的最新设备/分组 retained 快照，然后按设备间隔 1.5 秒依次发送 `ON` 或 `OFF`。

## 仓库布局

`jingjian-ha-addons` 必须作为独立 Git 仓库的根目录提交。不要把 monorepo 的上级目录添加到 Home Assistant 的仓库列表。

```text
jingjian-ha-addons/
├── repository.yaml
└── jingjian-sequence-gateway/
    ├── config.yaml
    ├── Dockerfile
    ├── requirements.txt
    ├── run.sh
    └── sequence_gateway.py
```

## 在 Home Assistant 中安装

1. 将本目录推送到 GitHub、GitLab 或 Gitee 等 Git 仓库。仓库根目录必须直接包含 `repository.yaml`。
2. Home Assistant 打开“设置 → 应用 → 应用商店”。
3. 点击右上角菜单，选择“仓库”，添加该 Git 仓库 URL。
4. 在应用商店中安装 `JingJian Smart Switch Sequence Gateway`。
5. 在“配置”中确认 MQTT 参数，保存后启动应用。
6. 打开“日志”，确认看到 MQTT 连接成功，并在设备和分组 retained 快照到达后看到空闲状态。

当前工作区尚未绑定远程 Git 地址；推送后请把实际仓库 URL 填入第 3 步。不要直接把本地 monorepo URL 配置给 Home Assistant。

## 默认配置

| 配置项 | 默认值 |
| --- | --- |
| MQTT 主机 | `core-mosquitto` |
| MQTT 端口 | `1883` |
| MQTT 用户名 | `cashier` |
| MQTT 密码 | `cashier` |
| 命令主题 | `jingjian/smart-switch/sequences/commands` |
| 状态主题 | `jingjian/smart-switch/sequences/status` |
| 定时方案主题 | `jingjian/smart-switch/schedules/<planId>` |
| 定时状态主题 | `jingjian/smart-switch/schedules/status` |
| 默认时区 | `Asia/Shanghai` |
| Zigbee2MQTT 基础主题 | `zigbee2mqtt` |
| 设备间隔 | `1500ms` |

设备与分组快照来自以下 retained 主题：

- `zigbee2mqtt/bridge/devices`
- `zigbee2mqtt/bridge/groups`

应用启动后会重新订阅这两个主题和命令主题，因此 MQTT 断线重连不会丢失订阅。命令主题必须是非 retained 消息；启动或重连时收到旧 retained 命令会被明确拒绝，不会重复开关机。

## 日志与故障排查

应用使用 `INFO`、`WARNING` 和 `ERROR` 记录完整业务链路。日志不打印 MQTT 密码和消息正文，只打印 Topic、请求 ID、方案 ID、revision、事件 ID、目标数量和 payload 字节数。

即时顺序开关机会记录：

- `sequence_received`：收到命令，包含 `request_id`、动作、分组/设备数量和 retained 标志。
- `sequence_started`：通过校验并开始执行，包含来源、动作和目标数量。
- `device_command`：逐台发布 Zigbee2MQTT 命令，包含设备 ID、friendly name、序号和 `ON/OFF`。
- `sequence_completed`：全部设备执行完成。
- `sequence_rejected`：参数、快照、并发锁或 retained 命令被拒绝。
- `sequence_failed`：设备命令发布或执行过程异常，附带 Python 堆栈。

定时方案会记录：

- `schedule_received`、`schedule_updated`、`schedule_ignored`、`schedule_deleted`：方案发布、版本替换、旧版本忽略和删除。
- `schedule_triggered`：到达方案的时间和星期，包含 `execution_key`。
- `schedule_failed`：方案目标、动作或执行异常。
- 同一轮定时执行还会复用 `sequence_started`、`device_command` 和 `sequence_completed`。

MQTT 生命周期会记录 `mqtt_start`、`mqtt_connected`、`mqtt_subscribed`、`mqtt_disconnected`、`mqtt_message_received` 和 `mqtt_message_failed`。可在 Home Assistant 的 App“日志”页搜索 `schedule_`、`sequence_` 或 `mqtt_` 快速定位问题。

## 定时方案协议

收银台将定时方案作为 retained 消息发布到 `jingjian/smart-switch/schedules/<planId>`。网关订阅 `schedules/+`，收到方案后按 `revision` 保存最新版本；删除方案时发布 `{"schemaVersion":1,"id":"<planId>","deleted":true}`。Broker 的 retained 消息是网关重启后的方案来源。

定时方案事件使用 `time`、`weekdays` 和 `commands` 字段。`weekdays` 使用 `mon` 到 `sun`，目标必须包含稳定的 Zigbee2MQTT IEEE 地址，网关触发时会从最新的 `bridge/devices` 快照重新解析 friendly name。这样设备改名后不需要重新生成旧方案。

```json
{
  "schemaVersion": 1,
  "id": "jingjian_cashier_plan1",
  "revision": 3,
  "timeZone": "Asia/Shanghai",
  "deleted": false,
  "events": [
    {
      "id": "period-1__on",
      "time": "09:00",
      "weekdays": ["mon", "tue", "wed", "thu", "fri"],
      "commands": [
        {
          "ieee": "0xaabbccddeeff0001",
          "deviceId": "0xaabbccddeeff0001",
          "payload": {"state": "ON"}
        }
      ]
    }
  ]
}
```

网关每秒扫描当前分钟的事件。同一个方案、revision、事件和本地日期分钟只会执行一次；执行时仍然使用即时命令的全局执行锁和 1.5 秒设备间隔。定时执行状态发布到 `jingjian/smart-switch/schedules/status`，包含 `scheduleId`、`revision`、`eventId` 和 `executionKey`。

## 收银台命令协议

```json
{
  "schemaVersion": 1,
  "command": "execute_sequence",
  "requestId": "cashier-1-20260918-001",
  "action": "turn_on",
  "groupIds": ["1"],
  "standaloneDeviceIds": ["0c4314fffe54b253"],
  "waitAfterMs": 1500
}
```

`groupIds` 会在 HA 执行开始时动态展开；独立设备 ID 使用 Zigbee2MQTT `ieee_address`，比较时会忽略 `0x`、冒号和短横线。分组和独立设备重叠时只执行一次。网关维护单个全局执行锁，同一时间只接受一条时序任务；重复 `requestId` 返回原结果，不会再次发送设备命令。

状态发布到 `jingjian/smart-switch/sequences/status`，例如 `starting`、`idle`、`accepted`、`running`、`completed`、`rejected` 和 `failed`。状态为 retained，便于收银台重连后恢复；设备控制主题 `zigbee2mqtt/<friendly_name>/set` 使用 QoS 1 且 `retain=false`。

## 手工验证

在 Home Assistant Terminal/SSH 中执行，先确认应用日志已经显示快照就绪：

```bash
mosquitto_pub -h core-mosquitto -p 1883 \
  -u cashier -P cashier \
  -t jingjian/smart-switch/sequences/commands \
  -q 1 \
  -m '{"schemaVersion":1,"command":"execute_sequence","requestId":"manual-001","action":"turn_on","groupIds":["1"],"standaloneDeviceIds":[],"waitAfterMs":1500}'
```

观察应用日志和设备状态。再次发送相同 `requestId` 不应产生第二轮设备命令；在第一轮执行尚未完成时从另一台收银台发送命令，应收到 `reason=already_running`。

## 故障排查

- `snapshots_not_ready`：检查 Zigbee2MQTT App 是否运行，并确认 `bridge/devices`、`bridge/groups` retained 消息存在。
- `group_not_found` 或 `group_empty`：检查收银台发送的分组编号和 Zigbee2MQTT 当前分组成员。
- `device_not_found`：收银台设备 ID 必须对应 `ieee_address`，不是 HA entity_id 或显示名称。
- `already_running`：等待当前时序结束；网关不允许多收银台并行控制。
- MQTT 已连接但设备不动作：检查 MQTT 用户权限、Zigbee2MQTT 基础主题和应用日志中的 `zigbee2mqtt/<friendly_name>/set` 主题。

应用升级时，在 Git 仓库提交新版本号和代码，Home Assistant 应用商店刷新后执行更新。配置保存在 Home Assistant，不会随应用镜像更新丢失。
