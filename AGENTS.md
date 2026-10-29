<claude-mem-context>
# Memory Context

# [DingoCache/launch-feat-wip-evict-v1] recent context, 2026-10-28 7:42pm PDT

No previous sessions found.
</claude-mem-context>

# DingoCache (dfkv) 开发与提交约定

本文件是 Codex、Claude Code 与贡献者共用的仓库开发规则，`CLAUDE.md` 通过 `@AGENTS.md` 导入，不重复维护。文件顶部的 `<claude-mem-context>` 块由插件自动生成，不属于项目规则，不要手工编辑，也不要依据其时间戳或内容判断规则；构建与 CI 的实际行为以配置为准。

核查基线：2026-10-26，`1b0eefd`（v2.33.3）、最近 150 条提交及现有工具配置；对照的代码、CI 与工具配置变化较大后，应重新核查并更新基线。下文区分三类内容：编码要求（项目决定）、已有自动检查、历史经验。

## C++ 规范：新增代码必须遵守

**新增 C++ 代码及实质修改的代码，必须遵守 [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)，并应用下列明确的项目适配。不能仅以“周围旧代码这样写”为由继续新增违规写法。**

适用顺序：用户当前明确指令 → 本文件明确的项目适配 → Google C++ Style Guide。排版由 `.clang-format` 决定，静态检查由 `.clang-tidy` 辅助；工具未检查某条规则不代表可以违反。现有协议、ABI、磁盘格式和公共接口不能因风格调整而被破坏。

### 基本要求

- 文件用 `snake_case.h` / `snake_case.cc`；测试放在 `test/<模块>/*_test.cc`，使用 GoogleTest。
- 两空格缩进，不用 Tab；通常左花括号同行。C++ 行宽按 `.clang-format` 的 80 列配置处理；长 URL 等允许例外按 Google 指南及格式工具处理。
- 头文件自包含，直接包含所需依赖，不依赖传递 include；新增头文件使用 `DFKV_<路径>_<文件>_H_` 形式的 include guard（如 `cache/foo_bar.h` 对应 `DFKV_CACHE_FOO_BAR_H_`）。现有头文件大多省略目录（如 `DFKV_SLAB_ALLOCATOR_H_`），个别使用 `#pragma once`，保持原样，不为此批量改旧头文件。
- 新增或实质修改的 `.cc` 首先包含对应头文件，再按 C 系统头、C++ 标准库、第三方库、项目头分组，各组按字母顺序排列。项目头路径相对 `src`，不用 `.` / `..` 路径别名。旧文件的 include 顺序不全符合，只整理本次改动涉及的 include，不重排整段。
- 类型和常规函数用 `PascalCase`，变量与参数用 `snake_case`，常量与枚举项用 `kPascalCase`。class 的数据成员（包括 public 成员）使用尾下划线；struct 的数据字段不加尾下划线。简单访问器可用 `snake_case`。
- **类型与引用（含单元测试）**：新增及实质修改代码优先写明确类型，通常不使用 `auto`；lambda 的匿名闭包等需要类型推导的场景可用 `auto`，其他例外应有具体理由。不为禁止 `auto` 而引入 `std::function` 或额外抽象。不机械地将 `const auto` 改为 `const auto&`：小对象按值返回时使用明确类型的 `const` 值；引用用于确实需要借用现有对象、避免不必要拷贝的场景，并核对生命周期。只调整本次涉及的代码，不批量改写旧代码。
- **枚举**：新增及实质修改的枚举显式指定底层类型，通常使用 `unsigned char`；每个枚举项必须显式赋值，一般从 `0` 开始。既有协议/ABI/磁盘格式规定的类型和值必须保留，不为统一风格改变它们，也不批量调整无关旧枚举。
- 使用具名命名空间（主要是 `dfkv`）；不新增 `using namespace` 指令。仅在 `.cc` 中用匿名命名空间限制内部实现可见性，不在头文件中使用匿名命名空间。
- 单参数构造函数按指南使用 `explicit`，确有隐式转换需求时说明理由；覆盖虚函数使用 `override` 或适当的 `final`；使用 `nullptr`。
- 用 RAII 管理资源；独占所有权优先 `std::unique_ptr`，共享所有权须有实际需求。裸指针不隐含转移所有权；指针、引用、异步回调涉及的生命周期应明确。
- 注释说明接口契约、前置条件、所有权、失败状态及不明显的理由；并发、内存序和持久化不变量必须说明。不以“只解释为什么”为由省略必要的接口说明。
- **代码保持简单。** 优先直白的控制流和小函数，避免不必要的抽象、模板技巧、过深嵌套和一行多件事；能用简单写法就不用复杂写法，不为“以后可能用到”预留机制。
- **确实复杂的逻辑必须在代码旁写注释**：包括状态机、并发与锁顺序、生命周期、内存序、位运算与偏移计算、回收与恢复流程、边界条件和为规避问题而写的特殊处理。注释要说明意图、前提和为什么这样做，而不是复述代码；改动逻辑时同步更新注释。无法写清楚的复杂逻辑，应先简化代码。

### 明确的项目适配与旧代码边界

- **语言版本仍为 C++17**：当前 Google 指南面向 C++20，但本项目由 CMake 限定为 C++17，不引入 C++20/23 特性，不因采用指南擅自升级工具链。
- **排版配置优先**：`.clang-format` 使用 Google 基线、80 列及 C++ 指针左贴。这是项目选定的格式，不另行凭个人习惯改动。
- **错误处理**：新增常规业务逻辑使用现有 `dfkv::Status` / `bool` 协议，不新增异常控制流。已有 RDMA/client 初始化边界确实存在 `throw`，修改这些路径时保留既有传播与捕获契约；不能宣称核心库完全没有异常。需要新增此类例外时，应明确设计理由和影响。
- **系统接口**：允许项目已有 Linux/POSIX、CUDA、verbs、io_uring 接口及必要的对齐、内存注册和 ABI 写法；与通用规则冲突的低层操作应局部封装并说明理由，不据此放宽普通业务代码。
- **兼容性优先**：现有 C ABI、对外接口和第三方 override 的名称可保留；不能为统一命名破坏调用方。修改旧函数时保留必要兼容名称，新增内部实现仍按规范。
- **渐进迁移**：新增文件整体合规；旧文件只调整本次实质修改范围及必要的相邻代码，不全仓重排、不顺手重命名无关接口。无关旧违规不阻断本次变更，但不得继续扩散。
- 其他偏离 Google 指南的写法必须在交付或提交说明中列出具体规则、技术理由和范围；已有代码中的例外不自动成为新增代码的许可。

### 检查与交付要求

- C++ 改动必须检查命名、头文件依赖、所有权、错误处理和接口契约，不能只依赖自动格式化。
- 新增 C++ 文件执行 `clang-format --style=file --dry-run --Werror <新增文件>`。
- 旧文件本身存在大量格式偏差（例如 `src/common/status.h` 就有告警），**不要对整文件执行 `clang-format -i`**。按目标范围只检查本次 diff：未暂存改动用 `git diff -U0 | clang-format-diff -p1`；已暂存改动用 `git clang-format --diff --staged`；工作区相对指定基线的改动用 `git clang-format --diff <基线提交>`（包含相对该基线的已暂存与未暂存变化）。`git clang-format --diff` 默认基线通常为 HEAD，但可被 Git 配置覆盖；需要确定范围时显式传入基线。上述命令只输出格式差异，确认后再应用；新建且未跟踪的文件按上一条单独检查。工具可能调整相邻语法结构，必须复核最终 diff，避免无关格式变化。
- 对实质修改的 C++ 文件，优先显式运行 `clang-tidy -p <本次构建目录> <文件>`，使用该目录的 `compile_commands.json`。`CMakeLists.txt` 已开启 `CMAKE_EXPORT_COMPILE_COMMANDS`，支持的生成器（如 Ninja、Unix Makefiles）会在配置时生成该文件。使用前核对源码路径、编译器、构建选项（尤其 RDMA/io_uring）与本次验证目标一致，配置变更后重新生成。仓库根目录同名文件可能是副本或符号链接，不能仅凭位置认定过期；只有确认其来源、链接目标（如适用）及配置与本次构建一致后才可使用。检查与本次改动相关的诊断，既有诊断单独记录，不为消警告进行无关重构；禁止无理由添加 `NOLINT` 或禁用检查。
- 工具版本以 LLVM 17.0.6 为当前已验证参考；使用其他版本时记录版本及兼容问题。缺工具、缺依赖或配置不兼容时明确列出未完成检查，不声称已合规验证。
- `.clang-tidy` 当前排除了 `google-*` 和 `readability-identifier-naming`，不能用其通过证明完整符合 Google 规范。`WarningsAsErrors: "*"` 只作用于 clang-tidy，不等于编译器全局 `-Werror`。
- **当前 CMake/CI 尚未自动执行 clang-format 或 clang-tidy。**上述检查是开发与审查要求，不是已有的自动门禁；接入自动门禁之前，交付说明必须列出检查范围、实际结果和项目适配，不能声称规则已被 CI 强制执行。

## 构建与测试

- CMake 要求 C++17，启用 `-Wall -Wextra`；CI 包含 GCC 与 Clang，修改不得新增编译警告。
- 基础 TCP 构建无需 GPU/RDMA 设备，依赖需预先具备：

```bash
cmake -S . -B build -DDFKV_BUILD_TESTS=ON -DDFKV_WITH_RDMA=OFF -DDFKV_WITH_URING=OFF
cmake --build build -j
ctest --test-dir build --output-on-failure
```

- `make build` 会先配置再编译，默认开启 RDMA 与 io_uring，需要 libibverbs/liburing 开发依赖；它不等同于仅执行 CMake 配置命令。`DFKV_WITH_URING=ON` 要求 RDMA 同时开启。
- `make test` 执行多个测试入口；`make test-cpp` 调用 CTest 并仅排除部分 Python/工具测试，不是严格只跑 C++。`make test-python` 用 unittest 收集 `test/python` 与 `integration/common/tests`。
- `make sanitize-test` 是 **ASan + UBSan**（内存错误与未定义行为）；竞态检查使用 **TSan**，构建与运行参数见 `.github/workflows/ci.yml` 的 `thread-sanitizer` 任务，两者不能替代。
- vLLM 连接器由独立 CI 任务执行 `python -m pytest -q -p no:cacheprovider -rs integration/vllm/tests`，需要相应 vLLM 与 Python 依赖，环境设置见该任务。
- 新测试必须确认进入真实执行入口：C++ 由 CMake/GoogleTest 收集，Python 可由 CMake、Makefile 或独立 workflow 收集，不要求每个 Python 文件都单独 `add_test`。用 CTest 列表或 pytest 收集结果核实。
- **每次提交的代码改动必须带单元测试**：新增功能、修复缺陷、性能或行为调整、重构，都要在同一提交内新增或更新对应测试，覆盖正常路径、边界和失败路径；修复缺陷先补能复现原问题的回归测试，再修代码；涉及并发、共享状态时覆盖并发场景。没有测试的代码改动不应提交。
- 仅以下提交可以不带新测试：纯文档、注释、版本号和发版元数据、CI/构建配置、纯格式化，以及不改变行为且已有测试充分覆盖的机械改动。此时在提交说明里写明不加测试的理由和依赖的既有测试。确因硬件缺失（真实 RDMA、GPU）等无法写自动化测试时，写明原因并补充可在无硬件环境运行的替代测试（如 mock 或 Soft-RoCE）。
- 按影响范围执行测试；共享状态/锁修改增加并发回归与 TSan，RDMA/io_uring 修改增加对应 ON 构建及目标数据路径验证。保留关闭可选功能时的默认构建能力。
- ctypes 集成测试通过 `DFKV_BUILD`、`DFKV_LIB` 指向此次构建产物，sanitizer 的动态加载限制及排除沿用对应配置。新增测试依赖同时检查普通 CI、sanitizer 与 release 环境。
- CI 还覆盖 Soft-RoCE、可移植产物、wheel、容器和部署配置。编译、回环、真实 HCA/GPU 验证分别记录；skip、缺硬件、未运行均不算通过。文档修改无需重跑全套构建。

## 提交与文档

- 动手前检查 `git status`、相关代码和测试，保留用户已有修改。只暂存本任务文件，不提交凭据、构建产物、缓存、压缩包、个人笔记或本地分析材料。
- 一次提交围绕一个完整逻辑变更，包含其单元测试和必要文档；不要把紧耦合测试/说明机械拆开，也不要混入无关重构、格式化和发版。
- 新提交标题使用 `type(scope): 英文祈使句摘要`，scope 可省略；常见 type 为 `fix`、`feat`、`perf`、`test`、`docs`、`ci`、`build`、`refactor`、`chore`。历史命名不完全统一，目前没有 commitlint 自动门禁。
- 复杂修复正文说明触发条件、根因、行为变化、兼容性和验证结果。只改客户端/连接器也应评估服务端请求与负载变化，不能机械声明“服务端行为未变”。
- 用户可见行为、参数、协议、指标及重要修复同步 `CHANGELOG.md` 的 `Unreleased` 和相关 README、`docs/`、usage；只修改受影响的内容，纯格式/测试修改不机械追加 CHANGELOG。
- 指标新增或改名更新 `docs/METRICS.md`，并检查 `deploy/observability/alerts.yml` 及相关查询/面板是否受影响。CI 检查配置与打包文件存在，不等于自动验证所有指标语义。
- 提交前执行 `git diff --check` 并审阅拟提交内容。交付说明列出实际构建、格式、静态检查、测试命令和结果，明确未验证项。没有创建提交、推送或发版的任务授权时，只交付修改。

## 发版契约

- 普通代码提交无需升版；发版通常单独使用 `chore: bump version to X.Y.Z`。
- 版本唯一来源为 `VERSION`，CMake 从中读取；发版同步 `integration/{common,lmcache,vllm}/pyproject.toml` 项目版本及 LMCache/vLLM 的精确 `dfkv-common==X.Y.Z` 依赖。
- 发布标签等于 `v<VERSION>`，提交须从 `origin/main` 可达；Actions 固定完整 commit SHA，已发布资产不可覆盖。依据为 `.github/workflows/release.yml` 和 `test/python/test_release_contract.py`。
- 修改版本或发布流程时运行 `python3 -m unittest discover -s test/python -p test_release_contract.py`，并检查二进制、动态库、wheel、镜像和打包流程。

## 项目不变量与历史经验

- **磁盘与 RAM**：共享 `SlabAllocator`，改磁盘策略必须核查 RAM；不淘汰 pinned slot，持久化失效与内存移除遵守既有协议。恢复、脏启动及格式兼容需验证，不能只保持结构体长度就宣称可回滚。
- **旧淘汰策略**：用户明确要求无条件保留 v2.33.0 的旧磁盘策略。新策略落地时保留可验证的旧模式。新旧策略的选择机制（编译选项或启动参数）尚未决定，也未在代码中实现，不把设计讨论当作现有功能。
- **RDMA/CUDA**：明确接收 credit、SEND fence、READ grant、QP/MR 和缓冲区所有权；失败、超时、取消、回收都需证明安全。外部 CUDA API 对照官方签名，不能只以返回成功判断调用正确。
- **连接器**：lookup 证明 LOAD 所需分片完整，SAVE 对应真实已提交的字节与状态；检查 TP/PP/DP/PCP/DCP、无新 block 的 prefill、MultiConnector 和跨 worker KV events。
- **线协议**：保持既有字段与数值含义，扩展要验证版本协商和旧端行为；`kResourceExhausted` 等本地状态不直接上线，检查 `DecodeRespVersion` 合法值范围。
- **日志与性能**：日志优先 `DFKV_LOG_*`，命令行输出可使用标准输出；热路径不新增无界日志或不必要锁。环境配置优先在初始化读取，新增项目变量使用 `DFKV_` 前缀。性能结论写明硬件、配置、负载、基线、资源预算与 p99，不能仅凭均值或计数。

## 依据与维护

- 编码规范：[Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)；工具文件为 `.clang-format`、`.clang-tidy`。指南未列入本文件的适用条款仍需遵守；其语言版本更新不自动升级本项目。
- 工程事实以 `CMakeLists.txt`、`Makefile`、`.github/workflows/`、`test/python/test_release_contract.py` 为准；源码示例不替代规范。
- 历史参考：`3b4d822`（测试注册）、`bda495a`（本次构建产物）、`62c981b`（发布测试依赖）、`ae0481c` / `c60bc4a`（RAM/磁盘一致性）、`389676c` / `f39d973`（credit/fence）、`8f233e5`（CUDA 参数）、`4ebfada`（分片检查）、`0c81bbc`（边界状态）。
- 规则变更区分新要求、已实现门禁和历史经验；新增项目适配必须明确理由与范围，不以旧代码、静态检查排除项或未经验证的提案悄悄放宽规范。
