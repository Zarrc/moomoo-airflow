# moomoo-watch × Airflow

把 moomoo-watch 原来那条 **Windows 计划任务 → `main.py`** 的线性流水线，
改写成一张 **Airflow DAG**（有依赖的图 + 可回填 + 有 UI）。

> **目标不是迁移生产，而是补一块能力。** 容器够不到本机的 OpenD（`127.0.0.1:11111`）
> 和 `claude.exe`，所以这里跑的是**离线 fixture 版**：数据源锁死 fixture、子代理关掉、
> 推送 simulate。**真机的 Windows 计划任务一行没动。** 详见 §6「与真机的对照」。

> **本仓库独立成仓**，与 [Zarrc/moomoo-watch](https://github.com/Zarrc/moomoo-watch) 配对：
> 把它克隆在 moomoo-watch 的**同级目录**，`docker-compose.yaml` 里的 `../moomoo-watch`
> 才找得到主仓。反方向**零依赖** —— 不玩 Airflow 的 moomoo-watch 用户完全不用知道本仓存在。

读完这份文档，你应该能说清楚 **DAG / task / 依赖 / schedule / backfill** 是什么，
并且有一个**真能跑起来**的例子（这就是这段学习的验收标准）。

---

## 1. 五个词，用这个项目当例子

| 词 | 一句话 | 在本项目里 |
|----|--------|-----------|
| **DAG** | 一张**有向无环图**：一堆任务 + 它们之间的先后关系。文件里定义、调度器定时扫 | `dags/moomoo_brief.py`（每天 06:30）、`dags/moomoo_alert.py`（每 30 分钟） |
| **task** | 图里的一个**节点** = 一次可独立重试的工作 | `collect`（取行情）、`rollup`（多周期回滚）、`push`（推送）… 共 10 个 |
| **依赖** | 谁能开始、要等谁 —— 用 `a >> b` 表达「a 完了才跑 b」 | `preflight >> [collect, calendar, macro]`；`packet >> summarize_brief >> rollup` |
| **schedule** | **多久跑一次**（cron 表达式）+ 时区 | `"30 6 * * *"` = 每天 06:30（`Asia/Shanghai`）；`"*/30 * * * *"` = 每 30 分钟 |
| **backfill** | **补跑历史**：为过去的时间区间各生成一次运行，把漏掉的日期补出来 | `airflow dags backfill moomoo_brief -s … -e …`（§4） |

几个配套概念，讲的时候容易漏：

- **`data_interval`（数据区间）**：一次运行「负责哪段时间的数据」。`start_date`/`schedule`
  定下来之后，Airflow 会自动把时间切成一段一段。本项目**只认 `data_interval_end`**，
  把它当成代码里的 `now`（§5 解释了为什么）。
- **`catchup`**：`False` = 别在我第一次上线时，把从 `start_date` 到今天的所有区间全补跑一遍。
  本项目两个 DAG 都是 `False`（想补历史就**显式** backfill，别让它自己偷偷跑几十次）。
- **`max_active_runs=1`**：同一个 DAG 同时只允许一个运行实例 —— 防止两次运行抢文件。
- **pool（并发池）**：给「不能并发的任务」限个名额。本项目建了一个 `moomoo_serial`（1 个槽），
  写 `outbox/` 与 `state.json` 的任务都排队进去（§5.2）。
- **XCom**：task 之间传**小**结果（路径、计数）。本项目的大对象走共享卷 JSON（`interim/`）。

---

## 2. 为什么要改（以及为什么选它当练习）

原来只有一条路：`计划任务 → run_brief.ps1 → main.py`，里面是一串 `if` 和一个循环。

| 问题 | 计划任务的做法 | Airflow 的做法 |
|------|----------------|----------------|
| **看得见吗** | 只有一个日志文件 | Graph / Gantt / 每次 task 的日志与耗时，UI 里全有 |
| **能并行吗** | 只能顺序跑 | 取数、日历、宏观**天然可以并行**，图直接表达出来 |
| **失败怎么办** | 要么整体退出码 0/1 | 每个 task 独立状态、独立重试（还能只重跑失败那个） |
| **补历史** | 得自己写循环 + 手改时钟 | `backfill` 一句话，按区间逐日重放 |
| **依赖可描述吗** | 藏在代码顺序里 | 显式 `a >> b`，一眼看出谁等谁 |
| **谁在跑** | 靠 `schtasks` 查 | UI + `airflow dags list-runs` |

> 这一段是**履历信号**的来源：数据岗的 JD 里写「熟悉 Airflow / 编排 / 调度」，
> 它要的就是上面这张表里的东西 —— 而**不能说清 DAG / task / 依赖 / schedule / backfill**
> 的人，是用过而没理解。

---

## 3. 目录结构

```
moomoo-airflow/                  # ← 本仓库根（独立 git 仓）
├── README.md                    # ← 你在读的这份（面向数据岗读者）
├── docker-compose.yaml          # postgres / airflow-init / scheduler / webserver（LocalExecutor）
├── config.airflow.yaml          # 容器专用配置：source=fixture / mode=simulate / agent.enabled=false
│                                #   + paths.* 全绝对路径（§5.1 解释为什么）
├── requirements-airflow.txt     # 空占位：fixture 模式无额外依赖（切 futu 时再解开注释）
├── .env.airflow.example         # AIRFLOW_UID / FERNET_KEY 示例（复制成 .env）
├── dags/
│   ├── .airflowignore           # 让 DagBag 别去扫 lib/（它不是 DAG）
│   ├── moomoo_brief.py          # DAG ① 每天 06:30：简报 + 多周期回滚
│   ├── moomoo_alert.py          # DAG ② 每 30 分钟：高危预警（含 gate 短路）
│   └── lib/                     # 轻适配层（DAG → core 的桥）
│       ├── env.py               # 引导 / 配置装配 / 可复现时钟 / 路径隔离
│       ├── io.py                # 原子写 JSON（阶段之间传大对象）
│       ├── stages.py            # 逐阶段的 task callable（t_preflight … t_push）
│       └── push.py              # 推送闸门（复刻 main.py 的 _push_draft）
├── docker/
│   ├── init-vault.sh            # 建「合成 vault」+ 打印 find_vault_root 自证
│   └── Dockerfile               # 可选，默认不用（fixture 模式不需要自定义镜像）
└── fixtures/
    └── due_week_start.json      # 备选：想在看 replay 时看到「周报出现」用它（§4 注意）
```

**`main.py` / `core/**` / `config.yaml` / `fixtures/**` / `tests/**` 一行没改** ——
它们全在同级主仓 [moomoo-watch](https://github.com/Zarrc/moomoo-watch) 里，本仓只以**只读挂载**引用。
所有适配都发生在 `dags/lib/` 这一层 —— 这是刻意的（§5）。

---

## 4. 跑起来

### 4.1 起服务

```bash
git clone https://github.com/Zarrc/moomoo-watch ../moomoo-watch   # 前提：主仓在本仓同级（已有则跳过）
cd moomoo-airflow                    # 本仓库根
mkdir -p .sandbox/vault            # Docker 会以 root 建目录；Linux 宿主注意属主
cp .env.airflow.example .env        # 可选
docker compose up -d
docker compose ps
docker compose logs airflow-init | grep find_vault_root     # ← 自证：应打印 /vault
```

打开 <http://localhost:8080>（`admin` / `admin`）。DAG 默认是**暂停**的，
点左侧开关（或 `airflow dags unpause moomoo_brief`）才会被调度器接管。

### 4.2 DAG 解析自检（先确认没问题）

```bash
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors   # 必须为空
docker compose exec airflow-scheduler airflow tasks list moomoo_brief --tree
docker compose exec airflow-scheduler airflow tasks list moomoo_alert --tree
docker compose exec airflow-scheduler airflow pools list | grep moomoo_serial   # ← 必须存在！
```

> 🚩 **池必须存在**：任务挂在一个**不存在**的池上时，Airflow **既不报错、也不调度它**
> —— DAG 会一直卡在「running 但有的 task 永远 queued」，UI 里看不出原因（R2）。
> `airflow-init` 已经建好它（并把上面那条 `grep -q` 当断言，失败就红）。


### 4.3 单跑一次：复现 SETUP §7.5 的验收

```bash
docker compose exec airflow-scheduler airflow dags trigger moomoo_brief \
  --exec-date "2026-10-03T06:30:00+08:00" --conf '{"seed_fixtures": true}'

ls .sandbox/vault/scratch/data/      # latest.json + report_2026-W40.json
cat .sandbox/vault/scratch/rollup/due.json
cat .sandbox/vault/Self/投资/预测台账/命中率.md   # rollup 写进沙箱 vault 的命中率页
```

期望：`session_date = 2026-10-02`，到期周期 `['weekly:2026-W40']`，出 `data/report_2026-W40.json`。

> ⚠️ **`--exec-date` 到底映射成哪个 `now`？**（这条容易差一天，务必读）
> 本项目取 `now = context["data_interval_end"]`。三种入口的语义：
> - **定时运行**：`logical_date` 是数据区间的**起点**，运行在**终点**触发 → 今天 06:30 触发的那次，
>   `data_interval_end` 就是**今天 06:30**（正是 `main.py` 的 `now`）。
> - **手动 trigger**（Airflow 2.x，维护者确认的既定行为）：数据区间是**往前倒推**的 →
>   `data_interval_end == 你传的 --exec-date`（**不是** exec-date + 1 天）。
> - **backfill**：按时间表切整区间 → `data_interval_end` = 区间终点。
>
> 所以「复现 SETUP 的 `--now 2026-10-03T06:30`」= `--exec-date 2026-10-03T06:30`。

### 4.4 幂等（有坑，先说清）

```bash
# 再 trigger 一次，**不带 --conf**（seed 只用于单次演示）
docker compose exec airflow-scheduler airflow dags trigger moomoo_brief \
  --exec-date "2026-10-03T06:30:00+08:00"
```

**诚实说明**：`daily` 的水位会挡住重复出日评；但**非 daily 周期在 `agent.enabled=false` 下
水位不推进** —— 这是 core 的**刻意规则**（`rollup.py`：报告全文没产出就不算完成，
否则该期会被静默丢失）。后果是：容器演示里 `report_2026-W40.json` 会被**同名重写**
（内容一致），`due` 不会是空集。

> 真正的 `due=[]` 幂等需要**子代理打开**（真机路径），或者用 §4.5 那种
> 「清空后首次运行」的 `bootstrap_mode: seed` 路线（它会把**所有**周期水位一次推到位）。

### 4.5 backfill：逐日重放那一周

```bash
docker compose exec airflow-scheduler rm -rf /vault/scratch/rollup /vault/scratch/state.json
docker compose exec airflow-scheduler airflow dags backfill moomoo_brief \
  -s "2026-09-28T06:30:00+08:00" -e "2026-10-02T06:30:00+08:00"
ls .sandbox/vault/scratch/rollup/daily/
```

`-e` 是**右开**的，所以会跑 4 次（logical 09-28 / 09-29 / 09-30 / 10-01），
对应 `session_date` 09-28 … 10-01，逐日出一条日记录。

**读结果时注意三件事**（都是 core 的正确行为，别以为是 DAG 坏了）：

1. **周报不出现** —— 首跑 `bootstrap_mode: seed` 会把水位**一次性推到本周**（weekly = `2026-W40`），
   于是这一周内周报不再是「到期」。这正是 seed 的设计目的（见 `SETUP.md` §7 的「首跑保护」）。
2. **会多出 10 条更早的日记录**（`2026-09-14 … 09-25`）—— 那是 core 的**回填（M2）**：
   从 `fixtures/klines_daily/` 补出「只有收盘、缺新闻/日历/告警」的日记录，带 `provenance.backfilled=true`。
3. **最后一天会多出 `report_2026-10.json` 与 `report_2026-Q4.json`** —— replay 走到 10-01 时
   跨了月/季，而水位里 monthly/quarterly 还停在 9 月/Q3 → 到期。它们只覆盖 1 个交易日（因为 replay 里
   October 只有 1 天），属于**部分周期**产物；报告里必须据此标「无」。

> 想反过来**看到周报出现**（备选路径）：把 `fixtures/due_week_start.json` 拷成
> `/vault/scratch/rollup/due.json`（水位停在上一周末），再 replay。代价是那份周报只覆盖 1 天
> —— 这就是「seed 水位」存在的理由。

### 4.6 parity：证明适配层没改语义

拿**传统单进程入口**跑同一时刻，和 DAG 产物比：

```bash
docker compose exec airflow-scheduler bash -lc '
  cd /vault/project && python main.py --source fixture --mode simulate --no-agent \
    --rollup --seed-fixtures --now "2026-10-03T06:30:00+08:00" --config /vault/scratch/config.yaml'
```

期望：`latest.json` 的 `trigger=alert`（CPI 落在预警窗口内 → 自动切换），
且**除时间戳外**产物一致。

> 📌 **这条由本次改造在本地实测过**（当时没有 Docker：直接调适配层，与 `main.py` 单进程比对）——
> 归一化时间戳后**逐字段相同**；其中 `latest.json` / 带时间戳的数据包 / `ledger.json` /
> 4 份日记录（09-28…10-01）**逐字节相同**。**在容器里请自己复跑一遍确认。**

> ⚠️ 直接 `diff` 会看到 `generated_at`（真实墙钟）等字段不同 —— 那是 §7 说的**已知限制**，
> 不是语义差异。要机器比对，先用脚本把 `generated_at` / `_updated_at` / `age_hours` 这类
> 抹成占位符再比。

---

## 5. 两个承重设计（不看懂这两条，容器一定跑不起来）

### 5.1 合成 vault 根：为什么路径全是绝对路径

`core` 有两处硬约束：

1. `config.find_vault_root()` 从 `core/config.py` **往上找**含 `.claude` 的目录 = vault 根。
2. `summarize.run` 要求 `data` / `outbox` / 报告包**都在 vault 之下**（`Path.relative_to`）。

如果把仓库直接挂到 `/opt/airflow/dags`（图省事最容易这么干），vault 根会解析到**容器根**，
而失败点会出现在 `summarize` **内部**（不在 preflight）—— 一个很难查的静默坑。

所以容器里的布局是**刻意造出来**的：

```
/vault/                                    ← 本仓 .sandbox/vault（RW；Windows 可做成指向 OneDrive 外的 junction）
├── .claude/agents/portfolio-watch.md      ← find_vault_root() 的锚点
├── Self/投资/{日评,周报,月报,季报,年报,预测台账}/   ← rollup 的沙箱落点（不碰真 vault）
├── scratch/{config.yaml,.env,data,outbox,logs,rollup,interim}/
└── project/                               ← 仓库本体，**只读**挂载（:ro）
```

解析链：`/vault/project/core/config.py` → 往上 → **`/vault`（有 `.claude`）= vault 根** ✓

于是 `paths.*` 全部写成**绝对路径**（`/vault/scratch/…`），靠的是 pathlib 语义：

```python
Path("/vault/project") / "/vault/scratch/data"  ==  Path("/vault/scratch/data")
```

绝对路径会**丢弃左边的 `PROJECT_ROOT`** → 仓库可以只读挂载，产物一个字节都不会写回
会被公开的目录。`fixtures` 留**相对**，正好指向只读的 `/vault/project/fixtures`。

`docker/init-vault.sh` 结尾会把 `find_vault_root()` 与每条解析出的路径**打印出来**，
把这类「静默失败」变成显式失败。

### 5.2 `outbox/` 的 mtime 串行：为什么需要 pool

`summarize._newest_outbox()` 判断子代理产出的方式是：**在 `outbox/*.md` 里按 mtime 取最新**。
两个写者同时往同一个 `outbox/` 写 → **静默取错标题**（不报错）。三层处理：

1. **同一写者只用一个 task**：`rollup.run` 内部要为 N 个周期各出一次稿，它必须**在同一个 task 里
   跑完**（所以 DAG 里只有一个 `rollup` task，不是一个周期一个 task）。
2. **不同写者隔离子目录**：适配层给 `brief` 与 `rollup` 各覆盖一个
   `cfg.raw["paths"]["outbox"] = …/outbox/<run_key>/<slot>` —— 纯赋值，core 一行不改。
3. **跨 DAG 兜底**：`packet` / `summarize_brief` / `rollup` / `push` 都挂
   `pool="moomoo_serial"`（1 个槽），两个 DAG 共用**同一个池**且都 `max_active_runs=1`。
   - `packet` 进池：`data/latest.json` 是**两个 DAG 共享**的写点（而 `packet.write` 不是原子写）。
   - `push` 进池：`state.json` 是**读-改-写**。
   - `interim/` 与 `outbox/<run>/…` 不用进池 —— 它们的 key 带 `dag_id`（见 `env.run_key`），
     天然隔离（`*/30` 与 `30 6 * * *` 会在 06:30 那个区间重合）。

---

## 6. 与真机的对照：将来怎么迁

| | 现在（容器 / fixture） | 真机（Windows 计划任务） | 迁真机的障碍 |
|---|---|---|---|
| 数据源 | `source: fixture`（读本地样例） | `source: futu`（连 OpenD） | 容器**够不到** `127.0.0.1:11111`（那是宿主交互式登录的网关） |
| 子代理 | `agent.enabled: false` | 调 `claude.exe` | 容器里没有 `claude.exe`（路径在宿主） |
| 推送 | `mode: simulate` | `mode: live` | 需要密钥（.env）、也可能不该让测试发真推送 |
| 调度 | Airflow（UI / backfill / 依赖图） | `schtasks` 两条任务 | — |
| 依赖 | 只 PyYAML / requests | + `futu-api` / `pandas` | 用 `docker/Dockerfile` 或 `_PIP_ADDITIONAL_REQUIREMENTS` |

**要真迁，路线是**：
① 把 OpenD 的网络可达性解决（例如 OpenD 跑在能连的机器上，容器走 `host.docker.internal`）；
② `docker/Dockerfile` 里安装 `futu-api pandas`，`config.airflow.yaml` 改 `source: futu`；
③ 把 `claude.exe` 换成容器内可用的调用方式（或把「出稿」这一步拆成一个 `BashOperator` 打外部端点）；
④ 先在 `mode: simulate` 下跑通，再切 `live`。**在②③没解决前，别把真机任务停掉。**

---

## 7. 已知限制（诚实边界）

- **不是生产迁移**：容器里 `source=fixture` / `mode=simulate` / `agent.enabled=false`。
  真机计划任务**一行没动**；本仓库整个删掉也不影响主仓的现有任务。
- **`summarize_brief` 在容器里恒为绿**（标注 `disabled`）：这是**刻意偏离** `main.py`
  （`main.py` 会把 `agent.enabled=false` 判成 `exit 1`）。因为演示就是要关掉子代理，
  否则每次都红 —— 而那条流水线本身是好的。除「超时」外的**真失败**仍会 raise 让 task 变红。
- **可复现的边界（R6）**：`now` 是可控的（`data_interval_end`），所以**日记录 / 周期键 / 打分 /
  水位 / 报告包**都可复现；但下面这些走**真实墙钟**、**不可复现**：
  - 数据新鲜度判定（`stale_data_minutes`）、K 线过期判定（`stale_klines.age_hours`）
  - `state.json` 的推送记录、各处 `generated_at` / `_updated_at` 时间戳
  - `provenance` 里的 `generated_at`
- **`gate` 是唯一一处刻意的行为增强**（`airflow.shortcircuit_alert`，默认开）：
  预警窗口内没有高险事件时短路掉整条链路，省掉一次无用的子代理调用。
  设 `false` 即回到与 `main.py` 完全一致的 parity 行为。
- **真的没有「进 GitHub 置顶」这一步** —— 那是对外发布动作，属于合并后的后续工作。
- **不改 wiki**：`Airflow` 目前只在 `wiki/数据架构/index.md` 占一行概念位，是否建独立概念页由用户定。

---

## 8. 冒烟自检（两层）

### 8.1 第一层 · 无 Docker 直跑（可选）

`dags/lib` **不依赖 airflow**（除了 `as_callable` 里的惰性导入），所以「适配层 + core」这段逻辑
可以脱离 Airflow 直接验：建一个临时「合成 vault」（含 `.claude` 锚点 + 仓库的 `core/fixtures` 拷贝），
设好 `MOOMOO_*` 环境变量，然后按顺序调 `stages.t_*`。

它验得了：挂载布局 / vault 根解析 / 路径重定向 / outbox 隔离 / rollup 水位与打分 / backfill 时序 /
parity。它**验不了**：DAG 解析、依赖树、调度、XCom、pool、重试 —— 那些只有真跑 Airflow 才算数。

### 8.2 第二层 · 容器实跑（2026-10-12 已验，Win11 + WSL3 + Docker Desktop）

| 验什么 | 结果 |
|---|---|
| init：vault 根解析 | `find_vault_root -> /vault`，所有路径落沙箱 ✅ |
| DAG 解析 | 2 张图、`import_errors: {}`、`moomoo_serial` pool 1 slot ✅ |
| brief 手动触发（`--exec-date 2026-10-03T06:30+08` + seed） | 10/10 success；报告包 `data/report_2026-W40.json`（agent 关 → 只出包不出全文，与 core 设计一致） |
| backfill 09-28…10-02 | 5 runs × 10 tasks 全 success ✅ |
| alert 触发 | gate 放行（fixture CPI 在窗内）→ 10/10 success ✅ |

容器实跑抓出**两个第一层抓不到的真 bug**（都已修，教训如下）：

1. **`airflow pools set` 的 description 在 2.10.5 是必填位置参数**（不传 → exit 2、init 直接红）。
   「不传最稳」这个判断是**错的** —— 版本歧义的正解是实测，不是回避。
2. **Airflow 任务进程里，root logger 严禁挂 stdout 的 StreamHandler**：Airflow 把任务的
   stdout/stderr 换成回灌 logger 的 `StreamLogWriter`，root 再挂写 stdout 的 handler 就成了
   `记录 → stdout → logger → 记录` 的**无限递归**（`RecursionError`；`handleError` 打的
   traceback 又写 stderr 假流，二次放大成 100KB+ 的复读日志行）。`lib/env.py::setup_task_logging`
   已改为：文件 handler（`watch.log`）照挂；console handler 仅在**非 Airflow 环境**（本地直跑）挂。
   root 的记录经 Airflow 的 root→stderr→task handler 通路自然进任务日志，无需自挂。

---

## 9. 风险速查（最可能踩的，多数是**静默**）

| # | 风险 | 缓解 |
|---|---|---|
| R1 | vault 根解析错 / `PROJECT_ROOT` 不在 vault 之下 | `/vault/.claude` 锚点 + 仓库挂 `/vault/project`；`preflight` 与 `init-vault.sh` 都打印 `find_vault_root()`。**最常见的错**：把仓库挂到 `/opt/airflow/dags` → 失败点在 `summarize` 而非 preflight |
| R2 | `outbox`/`state` 竞态（取错稿、丢记录，**不报错**） | §5.2：串行依赖 + 隔离子目录 + `pool moomoo_serial`。⚠️ 池**不存在**时任务被**静默不调度** → `airflow pools list` 必须见到它（2.10.5 实测：`pools set` 的 description 是必填位置参数，见 §8.2） |
| R3 | CST vs UTC（差 8h / 差一天） | `AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Shanghai` + `start_date` 显式 tz + **禁用 `{{ ds }}`** |
| R4 | naive/aware datetime 混用 | `now` 只从 `data_interval_end` 来；不用 `datetime.now()` 造 `now` |
| R5 | fixture 静态数据 vs 按日 backfill 的张力 | backfill 前清 `/vault/scratch/rollup`；报告读 `provenance.backfilled` 标注 |
| R6 | `now` 之外的时间源不可控 | 见 §7「可复现的边界」 |
| R7 | 并发写 `state.json` 丢记录 | `push` 进池 + `max_active_runs=1` |
| R9 | `.gitignore` 误伤 DAG | 只加一条 `.sandbox/`（**绝不**写裸 `dags/`，那会静默忽略 `dags/**` → DAG 发不出去） |
| R10 | DAG parse 期 import core | `lib/stages.py` 里所有 `from core import …` 写在函数体内、且在 `env.cfg_for()` **之后**；`dags/.airflowignore` 再加一道 |
| R12 | alert DAG 的 gate 与 parity 不同 | 只容器 demo 开，已在 §7 标注为唯一刻意的增强 + 给了关闭开关 |

---

*本文档随本仓库一起演进。改主仓 `core` 行为后，请回来核对 §5 与 `dags/lib/stages.py` 顶部那张行号对照表。*
