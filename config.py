"""muse2api 配置。"""
from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field

_DEFAULT_BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def dotenv_paths(base: str) -> list[str]:
    """按优先级从低到高：项目根 .env、data/.env。

    运行时生成/轮换的配置（如自动生成的 API Key）写进 data/.env：Docker 部署只把 data/ 挂成卷，
    写在项目根的 .env 会在重建容器后丢失。"""
    return [os.path.join(base, ".env"), os.path.join(base, "data", ".env")]


def _load_dotenv():
    """自动加载 .env 文件。

    - 项目根 .env：不覆盖已存在的非空系统环境变量。值为空的环境变量视同未设置：
      docker-compose 的 `MUSE2API_KEY=${MUSE2API_KEY:-}` 在宿主没设该变量时会注入空串，
      不能因此跳过 .env 里已保存的值。
    - data/.env：只存运行时由管理端写入的值（自动生成 / 轮换的 API Key、GITHUB_TOKEN），
      代表最近一次管理操作，覆盖一切来源（含 systemd EnvironmentFile、compose 注入的旧值），
      否则轮换后的 Key 重启即失效、已作废的旧 Key 重新生效。"""
    base = os.environ.get("MUSE2API_HOME") or _DEFAULT_BASE_DIR
    root_env, runtime_env = dotenv_paths(base)
    for env_path in (root_env, runtime_env):
        if not os.path.isfile(env_path):
            continue
        override = env_path == runtime_env
        try:
            with open(env_path, encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s or s.startswith("#") or "=" not in s:
                        continue
                    k, v = s.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'\"")
                    if k and (override and v or not os.environ.get(k)):
                        os.environ[k] = v
        except OSError:
            pass


_load_dotenv()


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _detect_chromium() -> str:
    """优先使用 MUSE2API_CHROMIUM；若未设置或路径不存在，则自动探测系统已安装的 Chromium/Chrome。"""
    configured = os.environ.get("MUSE2API_CHROMIUM", "").strip()
    if configured and (os.path.isfile(configured) or shutil.which(configured)):
        return configured
    candidates = [
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/snap/bin/chromium",
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "chromium",
        "chromium-browser",
        "google-chrome",
        "chrome",
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    ]
    for c in candidates:
        if os.path.isfile(c) or shutil.which(c):
            return c
    return configured or "chromium"


@dataclass
class Config:
    base_dir: str = field(default_factory=lambda: _env("MUSE2API_HOME", _DEFAULT_BASE_DIR))
    host: str = field(default_factory=lambda: _env("MUSE2API_HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(_env("MUSE2API_PORT", "18610")))

    # 浏览器
    chromium: str = field(default_factory=_detect_chromium)
    cdp_port: int = field(default_factory=lambda: int(_env("MUSE2API_CDP_PORT", "19210")))
    home_dir: str = field(default_factory=lambda: _env("MUSE2API_HOME_DIR", os.path.expanduser("~")))
    extra_path: str = field(default_factory=lambda: _env("MUSE2API_EXTRA_PATH", "/snap/bin"))
    site_url: str = field(default_factory=lambda: _env("MUSE2API_SITE", "https://muse.ai/"))
    login_wait: int = field(default_factory=lambda: int(_env("MUSE2API_LOGIN_WAIT", "60")))

    # 浏览器 profile 根目录。
    # 踩过的坑：snap 版 chromium 受 AppArmor 约束，只能读写 $HOME 下的路径。
    # 传 --user-data-dir=/opt/... 或 /tmp/... 不会报错，但会被静默忽略，
    # 浏览器回落到 ~/snap/chromium/common/chromium。
    # 所以必须落在 $HOME 下（不要用点开头的隐藏目录，snap 的 home 接口排除隐藏文件）。
    profile_root: str = field(
        default_factory=lambda: _env("MUSE2API_PROFILE_ROOT",
                                     os.path.join(os.path.expanduser("~"),
                                                  "muse2api-profiles")))

    # 鉴权
    api_key: str = field(default_factory=lambda: _env("MUSE2API_KEY", ""))
    # 可选的独立管理 Key：设置后 /admin/* 只认它，MUSE2API_KEY 只能调用 /v1/*（留空则两者共用 MUSE2API_KEY）
    admin_key: str = field(default_factory=lambda: _env("MUSE2API_ADMIN_KEY", ""))

    # 对外地址（管理页面显示「接入地址」用；留空则页面用当前域名兜底）
    public_base: str = field(
        default_factory=lambda: _env("MUSE2API_PUBLIC_BASE", ""))

    # 开源仓库（owner/name）：版本检测与一键在线升级指向它；默认本二开仓库，可用 MUSE2API_REPO 覆盖
    repo: str = field(
        default_factory=lambda: _env("MUSE2API_REPO", "biubiubiu125/muse2api").strip().strip("/"))

    # 生成
    image_timeout: int = field(default_factory=lambda: int(_env("MUSE2API_IMAGE_TIMEOUT", "240")))
    video_timeout: int = field(default_factory=lambda: int(_env("MUSE2API_VIDEO_TIMEOUT", "600")))
    chat_timeout: int = field(default_factory=lambda: int(_env("MUSE2API_CHAT_TIMEOUT", "300")))
    max_concurrency: int = field(default_factory=lambda: max(1, int(_env("MUSE2API_CONCURRENCY", "2"))))
    # 每账号并发槽位（默认 1 = 同账号串行；放开前务必实测 /api/session 续签是否会踢旧会话）
    per_account_concurrency: int = field(
        default_factory=lambda: max(1, int(_env("MUSE2API_PER_ACCOUNT_CONCURRENCY", "1"))))
    # 软隐身总开关：UA 跟随真实 Chrome、去 HeadlessChrome、按账号注入指纹噪声、屏蔽 webdriver。调试可设 0 关闭。
    stealth: bool = field(default_factory=lambda: _env("MUSE2API_STEALTH", "1").strip() != "0")
    # 配了代理但账号未显式设时区时的默认时区（Muse 仅美/加，默认用美东；设空则不覆盖时区）
    default_timezone: str = field(default_factory=lambda: _env("MUSE2API_DEFAULT_TIMEZONE", "America/New_York"))

    # 参考图输入默认只允许调用方直接提供数据和公网 URL。
    # 仅可信单用户部署可显式设为 1 恢复服务器本地文件或私网抓取。
    ref_image_local_files: bool = field(
        default_factory=lambda: _env("MUSE2API_REF_IMAGE_LOCAL_FILES", "0").strip() == "1")
    ref_image_private_fetch: bool = field(
        default_factory=lambda: _env("MUSE2API_REF_IMAGE_PRIVATE_FETCH", "0").strip() == "1")

    # 工具调用（function calling）协议适配开关，**默认关闭**。
    # 实测 muse.ai 的助手会明确拒绝输出"伪工具调用"（原话：「我不会按这种 JSON
    # 格式输出'伪工具调用'」），强行注入协议反而会污染正常回答。
    # 所以默认只做「参数兼容」（客户端传 tools 不会 422），不注入、不解析。
    # 若目标模型行为有变，可设 MUSE2API_TOOL_PROTOCOL=1 打开这层适配再试。
    tool_protocol: bool = field(
        default_factory=lambda: _env("MUSE2API_TOOL_PROTOCOL", "0").strip() != "0")

    # 保活设置：是否对已禁用的账号执行自动保活，**默认关闭**。
    # 开启后，被管理员在后台手动禁用的账号仍会由后台守护进程执行自动保活与 VM 唤醒，
    # 保持会话活性不失效，但依然不会参与正常业务生图、生视频与对话轮转调度。
    keepalive_disabled_accounts: bool = field(
        default_factory=lambda: _env("MUSE2API_KEEPALIVE_DISABLED_ACCOUNTS", "0").strip() != "0")

    @property
    def data_dir(self) -> str:
        return os.path.join(self.base_dir, "data")

    @property
    def media_dir(self) -> str:
        return os.path.join(self.data_dir, "media")

    @property
    def download_dir(self) -> str:
        return os.path.join(self.data_dir, "downloads")

    @property
    def profile_dir(self) -> str:
        """生成用浏览器 profile。"""
        return os.path.join(self.profile_root, "generate")

    @property
    def accounts_file(self) -> str:
        return os.path.join(self.data_dir, "accounts.json")

    @property
    def tasks_file(self) -> str:
        return os.path.join(self.data_dir, "tasks.json")

    def ensure_dirs(self):
        for d in (self.base_dir, self.data_dir, self.media_dir, self.download_dir,
                  self.profile_dir):
            os.makedirs(d, exist_ok=True)


CFG = Config()
