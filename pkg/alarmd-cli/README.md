# alarmd-cli

面向运维取证的独立 Go 客户端。服务端负责操作目录、输入合同、预算和证据判定；客户端只处理环境登录、通道调用、凭据与输出。没有内置业务 operation，也不访问 K8s、Redis 或任意内部 URL。

## 安装

首版支持 macOS arm64 和 Linux amd64；运行只需单个二进制。下载对应版本的归档及 `SHA256SUMS` 后先校验，再解压到自己的可执行目录：

```sh
# macOS；Linux 可改用 sha256sum -c SHA256SUMS
shasum -a 256 -c SHA256SUMS
tar -xzf alarmd-cli_<version>_<os>_<arch>.tar.gz
./alarmd-cli --version
./alarmd-cli --help
```

`--help` 与 `--version` 无需配置或网络。制品是否已部署与服务端是否可达需要另行验收。

## 从零开始

先打开已知环境的 OB CLI 授权页面，输入部署管理员密钥，确认后生成一次性授权码。alarmd 自己校验该密钥，授权范围为部署级运维取证。管理员密钥通过部署 Secret 配置为 `cli.admin_key`，只在页面当次授权时输入，不保存到 CLI 配置；CLI 仅持有兑换后的短时会话。运行下列命令后粘贴，终端不回显；也支持从受保护 stdin 读取，授权码不接受普通命令行参数。

```sh
alarmd-cli auth login
alarmd-cli profile list
alarmd-cli discover --env <environment_id>
alarmd-cli describe <operation> --env <environment_id>
alarmd-cli invoke <operation> --env <environment_id> --input '{"field":"value"}'
# 参数也可以读取已有的 JSON 文件
alarmd-cli invoke <operation> --env <environment_id> --input @input.json
```

登录自动导入授权码中的稳定 `environment_id`、名称和入口；可选 `auth login --env <id>` 用于核对环境。其余远程命令必须显式传 `--env`。`profile use <id>` 仅记录人工偏好，不会给远程命令隐式选择环境。服务端更新操作目录后客户端无需升级；先读 discover 摘要，按需读 describe 中的 schema、limits、parameter_sources 和示例。

每次 invoke 在本进程先 describe 一次，再携带当前 revision 调用一次。没有持久 schema 缓存、完整客户端 schema 校验器或自动重试。`catalog_changed` 说明合同变化，应重新查阅 describe 后决定是否再次调用。`next_call` 只是建议，客户端不会自行执行。

## Slot 取证

需要服务端的操作目录里有 `slot.get` 和 `slot.query`（用 `discover` 查看）。先查看操作合同：

```sh
alarmd-cli describe slot.get --env <environment_id>
alarmd-cli describe slot.query --env <environment_id>
```

从 `object.get` 的 `next_call` 取得 `slot.get` 参数；后者只读取保留证据和查询预览，再从其 `next_call` 选择一个 `slot.query`，将 `params` 原样保存为 JSON 文件并通过 `--input @文件名` 调用。大结果从 `meta.result_file` 读取。`slot.query` 默认由当前 owner 执行，也可按 describe 的合同显式传 `replica` 选择实例。

查询结果的 `kind=requery_now` 表示按保留条件发起的本次重查，可能包含迟到数据，不能当作原 Slot 的完整历史输入或告警重放。返回 partial（退出码 3）时检查 `evidence.limitations`、查询完成状态和截断标记；历史合同缺失时不能用当前配置替代。

## 部署本身的 K8s 读数

需要带 K8s 只读取证的服务端，以及 chart 为 alarmd 渲染的只读 Role（CLI 开启即带）。三个操作都由入口副本用自己 Pod 的 ServiceAccount 只发 GET，只读 alarmd 自己的 Deployment：

```sh
# Deployment 状态，各 Pod 的阶段、就绪、重启次数与上次退出原因
alarmd-cli invoke k8s.pods --env <environment_id>
# Deployment、ReplicaSet、Pod 上的事件，新的在前；可只看一个 Pod
alarmd-cli invoke k8s.events --env <environment_id> --input '{"pod":"<pod>"}'
# 容器日志末尾；previous=true 读上一次运行（崩溃前）
alarmd-cli invoke k8s.logs --env <environment_id> --input '{"pod":"<pod>","previous":true,"lines":200}'
```

读不到时按失败码区分：`k8s_rbac_forbidden`（Role 没建或被关）、`k8s_service_account_not_mounted`、`k8s_apiserver_unreachable`、`k8s_not_found`（含从未写过的 previous 日志）、`k8s_not_in_scope`（不是 alarmd 的 Pod 或容器）等，不会以空列表冒充"没有事件"。所有副本都挂掉时这条路读不到，只能用 kubectl。

## 会话与环境

```sh
alarmd-cli auth status --env <environment_id>
alarmd-cli auth logout --env <environment_id>
# 只有核实同一环境确实迁移入口后，才明确重新绑定
alarmd-cli auth login --rebind
```

只有实际 invoke 请求携带 `renew_if_due=true`，是否续期由服务端准入决定。discover、describe、status 不主动续期。没有 daemon、refresh token 或保活心跳。本地 `expires_at` 仅是提示：另一进程可能已经续期，服务端始终负责裁决。

logout 先请求远端撤销，再清理匹配的本地凭据。网络失败也会按会话 ID 与 token 摘要条件清理，同时明确 `remote_revocation_confirmed=false`、退出码 1。旧会话 A 的迟到响应或 logout 不能覆盖或删除新登录的 B；同一会话的并发 expiry 回写只保留较新期限。退出后保留无凭据的环境入口绑定及既有证据文件。

HTTP 或 HTTPS 由部署入口决定，CLI 按授权码内的原协议连接并保存，不要求公网证书，不自动升级、降级或跟随重定向。HTTP 直接运行 `auth login` 即可，结果的 `meta.client_transport` 标记 `encrypted=false`。

HTTPS 默认使用系统信任库。私有 CA 可通过 `auth login --ca-cert /absolute/path/ca.pem` 按环境保存并追加信任根，仍校验证书链和主机名；后续请求需要保留该 CA 文件。接受自签证书或域名不匹配时，使用 `auth login --insecure-tls`，后续该环境请求跳过证书链和主机名校验。两选项互斥且仅适用于 HTTPS，授权码和服务端不能自行开启；重新成功登录不带 `--insecure-tls` 即恢复正常校验。`profile list` 和取证回执显示实际模式。

兑换不会附带已有 token、cookie 或自定义身份头。入口不接受 URL userinfo/query/fragment 和路径穿越。同 environment_id 的 origin（含协议）改变时，使用 `auth login --rebind` 确认新的环境绑定。

## 输出与证据

除 help 外 stdout 都是 JSON；进度与非致命提示写 stderr。

| 退出码 | 含义 |
| --- | --- |
| 0 | 完整调用成功 |
| 3 | 部分证据，检查 `evidence.limitations` |
| 1 | 调用、协议、配置或落盘失败 |
| 2 | 命令或输入无效 |

业务健康状态保留在 `result`，不与调用成败混淆。完整脱敏通道响应一次原子保存，并在 `meta.result_file` 返回绝对路径。未知可选字段和大整数保留；stdout 最多 20 KiB，必要时省略 result 并标记 `result_omitted=true`，直接读取结果文件即可，不需要重查服务端。客户端删除秘密字段并替换已知 token/grant，服务端仍须执行自己的安全字段投影。

凭据位于平台用户配置目录下的 `alarmd-cli/profiles.json`；`ALARMD_CLI_CONFIG_DIR` 可指定独立目录。目录权限 0700、凭据/锁/结果文件 0600。响应在 `results/` 下保留，客户端不自动删除证据。勿把凭据配置目录上传到工单或公开仓库。

固定边界：网络期限 30 秒；服务端响应最大 8 MiB，超限拒绝解码；参数 JSON 最大 1 MiB；授权码最大 64 KiB。没有不经服务端校验的任意 endpoint、operation 枚举或业务诊断逻辑。此版本不承诺 Windows。

## 构建与验证

需要 Go 1.23 或更新版本：

```sh
go test -race ./...
go vet ./...
go build -buildvcs=false -trimpath -ldflags '-X main.version=dev' -o alarmd-cli .
sh scripts/release.sh v0.1.0
```

发布脚本在 `dist/<version>/` 生成 darwin-arm64、linux-amd64 归档、版本与源码回执，以及 `SHA256SUMS`。测试覆盖 TLS 兑换、前缀路由、环境/scope、重定向拒绝、过期提示下续期、部分大结果与脱敏、revision 变化不重试、迟到响应/CAS、未知远端撤销，以及真实二进制的零配置 help/version。测试依赖本机随机端口，不连接线上系统。
