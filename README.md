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
| Zigbee2MQTT 基础主题 | `zigbee2mqtt` |
| 设备间隔 | `1500ms` |

设备与分组快照来自以下 retained 主题：

- `zigbee2mqtt/bridge/devices`
- `zigbee2mqtt/bridge/groups`

应用启动后会重新订阅这两个主题和命令主题，因此 MQTT 断线重连不会丢失订阅。命令主题必须是非 retained 消息；启动或重连时收到旧 retained 命令会被明确拒绝，不会重复开关机。

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
