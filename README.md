# Cadence Forge · 音视频成片服务

Go HTTP 后端，以真实音视频字节为输入，提供版本化剪辑、字幕时间映射、MP4/WAV加工和可核验交付。没有Web前端。

## 运行

需要 Go 1.24+、Python 3（仅运行有界工作流检查）及带 libx264、AAC、loudnorm、tile、showwavespic 的 FFmpeg/ffprobe。当前本机实际验证 Go 1.27、FFmpeg/ffprobe 9.0.2；Docker构建定义采用Go 1.24和Debian媒体包，镜像运行尚需目标环境独立验证。

```sh
go build -trimpath -o bin/forged ./cmd/forged
./bin/forged
```

默认监听 `127.0.0.1:8734`，数据根 `var/cadence-forge`。可以设置 `FORGE_ADDR`、`FORGE_ROOT`、`FORGE_WORKERS`（1..8，默认2）、`FORGE_UPLOAD_TTL_HOURS`（1..720，默认24）、`FFMPEG_BIN`、`FFPROBE_BIN`。同一数据根仅允许一个服务进程。服务作为受信任环境中的媒体后端运行，面向公网接入前应部署认证和网络边界。

使用现有媒体工具运行所有真实工作流：

```sh
python3 scripts/workflow_check.py --workflow all
```

检查在临时数据根内生成4秒校准音画，不依赖外部素材或在线服务，完成18条工作流，包括真实强制终止及恢复。`--workflow`可选择运行清单中的某条工作流；所需的前置接收、版本编辑和渲染也会执行。初始化基线不含自动测试套件，testing为deferred；这里提供有界生产工作流核验，不声明测试命令。

## 调用顺序

1. `POST /forge/v1/sources?name=校准片`：请求体为媒体原始字节，返回素材ID、SHA-256与流事实。
2. `POST /forge/v1/recipes`：例如 `{"name":"审片","format":"mp4","width":640,"height":360,"fps":24,"sample_rate":48000,"normalize":false,"target_lufs":-16}`。
3. `POST /forge/v1/sheets`：`{"name":"剪辑","spans":[{"source_id":"素材ID","in_ms":0,"out_ms":1500,"gain_db":0}],"cues":[{"source_id":"素材ID","start_ms":500,"end_ms":1000,"text":"校准"}]}`。
4. `GET /forge/v1/sheets/{id}/preview?recipe_id={id}`：预览当前最新修订的累计区间及WebVTT。片段按半开区间求交；同源重复出现分别映射。
5. `POST /forge/v1/renders`：提供 `Idempotency-Key`，正文 `{"sheet_id":"剪辑ID","sheet_revision":1,"recipe_id":"配方ID","recipe_revision":1}`。必须显式选择修订，提交冻结引用与时间计划；预览最新版本不替代提交选择。
6. `GET /forge/v1/renders/{id}`：查看排队、运行、成功、失败或取消及尝试历史。
7. `GET /forge/v1/renders/{id}/artifact`：下载检验通过的媒体；`?captions=1`下载字幕。`POST .../verify`重新核验；`GET .../bundle`返回媒体、字幕及冻结说明组成的ZIP。

## 可恢复接收（分块上传）

大文件或弱网下使用分块会话，网络恢复后只补缺失分块，而不是重传整个素材。会话本身**没有素材身份**：它只描述正在接收的字节，不出现在 `/state` 或 objects 中；只有整文件 SHA-256 与 ffprobe 都通过并写入 journal 后，才产生可被剪辑引用的 Source。分块看似齐全但内容未确认时，绝不会提前成为素材。

1. `POST /forge/v1/uploads`：JSON 声明 `name`、`size`、`chunk_size`，可选整文件 `content_sha256` 与覆盖全部分块的 `chunk_sha256` 清单。返回随机会话 ID、分块数与全量 `missing`。
2. `PUT /forge/v1/uploads/{id}/chunks/{index}`：请求体为该分块原始字节，服务器按声明偏移落盘并独立重算 SHA-256，返回最新 `received`/`missing`。同索引并发重传被串行化：字节相同则幂等汇合，字节不同则冲突拒绝，不会串改同一身份。
3. `GET /forge/v1/uploads/{id}`：查看已收与缺失分块，上传者据此只补缺失内容；重启后视图由磁盘标记重建，依然真实。
4. `POST /forge/v1/uploads/{id}/complete?name=...`：唯一提升门。校验全部分块、整文件大小与整文件 SHA-256，硬链接进内容寻址 objects，再 ffprobe 并登记 Source；同字节内容与一次性 `POST /sources` 汇合到同一素材 ID。该调用幂等：字节已提升但探测或登记失败时，重试直接复用已提升对象，无需重传任何分块。
5. `DELETE /forge/v1/uploads/{id}`：放弃会话并删除其目录，放弃的会话不可能再被提升为素材。

校验成本是分层选择的：每块仅做一次流式 SHA-256（随块确认、成本与块大小成正比），声明 `chunk_sha256` 时额外逐块比对清单；最终身份只在 complete 时做一次整文件顺序 SHA-256，避免为每个分块承担整文件重算。临时空间上，每个会话只有一个按源大小 truncate 的稀疏 `data` 文件（未写部分不占磁盘块）加一个在途分块临时文件，提升用**硬链接**共享 inode，因此不需要保存完整的第二份副本。默认保留 24 小时（`FORGE_UPLOAD_TTL_HOURS` 调整），启动时清扫超时未推进的会话以回收临时空间。

## 编辑与恢复

`PUT /sheets/{id}`及`PUT /recipes/{id}`使用十进制修订号 `If-Match: 1`。本服务的该头为应用级修订约定，并非通用HTTP条件请求实现。保存只追加历史，过期写入409。输入中的服务管理ID与修订由服务生成。

`POST /renders/{id}/cancel`撤回排队或运行中的交付；成功确认后该任务不能新提交成片。进程停止是后续动作，并不承诺确认瞬间资源已全部释放。成功成片不可再取消。`POST /renders/{id}/retry`仅接受failed，最多三次尝试，沿用原冻结输入。重启时running变为failed/interrupted，等待显式重试，queued继续排队。

`GET /events?after=0`返回至多200条严格后继事件。序号是持久顺序，时间戳仅用于解释。`GET /state`返回当前一致快照，适合受信任客户端。

## 分析入口

`POST /sources/{id}/loudness`测量源LUFS、真峰值和动态范围，静音或不可测结果使用null并指示silent。`GET /sources/{id}/visual?mode=storyboard`生成四格图；`mode=waveform`生成640×120单声道概览。它们是有成本的同步分析，请求受独立超时约束；波形合并声道可能隐藏反相内容，故事板不等于逐帧审片。

## 支持范围与限制

单素材最多64 MiB、约120秒、3840×2160和8声道。分块接收的单块为1 KiB..8 MiB、单次上传最多10000块；剪辑最多32段、120秒、500条字幕，段长至少100ms；输出MP4/H.264/AAC或WAV/16位PCM，采样率44.1或48kHz。MP4要求每段有视频；无声视频生成对应静音。WAV要求每段有声音。视频保持完整画面并留边，统一像素比例与帧率，声音统一立体声。归一化为单遍响度滤镜，不保证任何短片都精确达到目标值。

时长基准为毫秒，编码后的帧与采样可能有容差。检验检查流、尺寸、采样率、时长与摘要，不代表内容正确、逐帧同步或主观质量已通过人工审片。禁止容器中的远程协议与非白名单封装，但媒体进程仍需要部署侧隔离。

接收先耐久保留字节再探测登记，失败可能留下未引用对象。成功产物先检验、耐久提升，再提交可见事实；崩溃可能留下未登记产物。当前不自动回收这些内容。`DELETE /sources/{id}`只退休无历史引用的素材记录，保守保留对象；历史剪辑、字幕和渲染血缘均阻止退休。

状态写入使用同文件系统临时文件、fsync、替换与父目录同步；目录同步失败时服务拒绝后续写入并要求恢复核对。对象及产物与状态分阶段提交，已生成目录不能自行视为交付成功。请备份整个数据根，并在停止唯一服务实例后进行一致复制。

## 容器

```sh
docker build -t cadence-forge .
docker run --rm -p 8734:8734 -v cadence-forge-data:/data cadence-forge
```

运行身份为10001。这里的容器定义不是跨平台运行通过声明；实际验证范围与本机工作流结果见交付记录。
