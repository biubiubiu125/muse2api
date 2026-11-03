"""一键离线运行 tests/ 下的全部测试：python3 tests/run_offline.py

只用标准库。逐个以子进程运行 tests/ 下的 test_*.py 与 test_*.mjs，cwd 一律为仓库根：

    python3 tests/test_xxx.py                         .py 用运行本脚本的同一个解释器
    python3 tests/test_vm_wait.py engine.py --assert  需要参数的脚本登记在 EXTRA_ARGS
    node tests/test_xxx.mjs                           PATH 上找不到 node 时记 SKIP

每个测试的判定：

    PASS     退出码 0
    SKIP     退出码 0，且输出里有以 "SKIP:" 开头的行（脚本自报缺依赖）
    FAIL     退出码非 0
    TIMEOUT  超过 --timeout（默认 600s）；连同它拉起的子进程（如 Chromium）一起停掉
    XFAIL    登记在 XFAIL 清单里的已知失败：失败时记 XFAIL，不影响退出码
    XPASS    登记在 XFAIL 清单里却通过了：退出码记 1，提示把它移出清单

退出码：有 FAIL / TIMEOUT / XPASS 时为 1，否则为 0；-k 一个测试也没匹配上时为 2。

    python3 tests/run_offline.py                  # 全部
    python3 tests/run_offline.py -k harness       # 只跑文件名含 harness 的（不区分大小写）
    python3 tests/run_offline.py --list           # 只列出测试与命令，不运行
    python3 tests/run_offline.py --timeout 900 --tail 80

POSIX 下每个测试在独立的进程组里运行。超时先只对测试主进程发 SIGINT：Python 测试抛出
KeyboardInterrupt，跑 finally 里的清理（关浏览器、删临时目录），并在输出末尾留下卡住处的栈；
最多等 5s 让它退出，然后把整组 SIGKILL，清掉它没关掉的进程。测试正常退出 1s 后组里
还有进程（如没关掉的 Chromium）也整组杀掉并在 NOTE 里注明，免得它占着端口影响后面的测试。
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TESTS_DIR.parent
POSIX = os.name == "posix"

# 需要额外命令行参数的测试（参数按仓库根解析）。
EXTRA_ARGS: dict[str, list[str]] = {
    "test_vm_wait.py": ["engine.py", "--assert"],
}

# 已知失败清单：文件名 -> 原因与预计修复它的任务号（tasks.md）。
# 失败记 XFAIL、不影响退出码；一旦通过就记 XPASS 并让退出码为 1，由修好它的任务删掉条目。
XFAIL: dict[str, dict[str, str]] = {}

VERDICTS = ("PASS", "FAIL", "TIMEOUT", "XFAIL", "XPASS", "SKIP")
BAD = ("FAIL", "TIMEOUT", "XPASS")
LEFTOVER_GRACE = 1.0  # 测试退出后，给它留下的子进程自行退出的时间（秒）
STOP_GRACE = 5.0      # 超时或 Ctrl+C 发出 SIGINT 后，等测试自行清理退出的时间（秒）


@dataclass
class Result:
    name: str
    verdict: str
    seconds: float = 0.0
    note: str = ""
    output: str = ""


def find_node() -> str | None:
    return shutil.which("node")


def discover(pattern: str | None = None) -> list[Path]:
    """tests/ 下的 test_*.py 与 test_*.mjs，按文件名排序；pattern 为不区分大小写的子串。"""
    found = sorted(p for p in TESTS_DIR.iterdir()
                   if p.is_file() and p.name.startswith("test_") and p.suffix in (".py", ".mjs"))
    if pattern:
        found = [p for p in found if pattern.lower() in p.name.lower()]
    return found


def command(path: Path, node: str | None) -> list[str]:
    """运行该测试的命令，cwd 为仓库根。"""
    rel = path.relative_to(REPO_ROOT).as_posix()
    if path.suffix == ".mjs":
        return [node or "node", rel]
    return [sys.executable or "python3", rel, *EXTRA_ARGS.get(path.name, [])]


def missing_runtime(path: Path, node: str | None) -> str | None:
    return "node not found" if path.suffix == ".mjs" and not node else None


def shown(cmd: list[str]) -> str:
    return " ".join([Path(cmd[0]).name, *cmd[1:]])


def _group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _kill_tree(proc: subprocess.Popen) -> None:
    """杀掉测试进程和它拉起的子进程（POSIX 下是整个进程组）。"""
    if POSIX:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        return
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL, check=False)
    if proc.poll() is None:
        proc.kill()


def _stop_tree(proc: subprocess.Popen) -> None:
    """先只对测试主进程发 SIGINT、等 STOP_GRACE 秒让它自行清理，再整组杀掉（非 POSIX 直接杀）。

    不对整组发 SIGINT：浏览器要留给测试自己的 finally（close_app → engine.stop()）去关，
    先被信号打死的话，清理会卡在已断开的 CDP 连接上。"""
    try:
        if POSIX:
            try:
                os.kill(proc.pid, signal.SIGINT)
            except (ProcessLookupError, PermissionError):
                pass
            try:
                proc.wait(timeout=STOP_GRACE)
            except subprocess.TimeoutExpired:
                pass
    finally:  # 等待期间再按一次 Ctrl+C 也照样整组杀掉
        _kill_tree(proc)


def _reap_leftovers(proc: subprocess.Popen) -> bool:
    """测试退出后组里还有进程：等 LEFTOVER_GRACE 秒仍在就整组杀掉，返回 True（仅 POSIX）。"""
    if not POSIX:
        return False
    deadline = time.monotonic() + LEFTOVER_GRACE
    while _group_alive(proc.pid):
        if time.monotonic() >= deadline:
            _kill_tree(proc)
            return True
        time.sleep(0.05)
    return False


def _exit_note(code: int) -> str:
    if code < 0:
        try:
            return f"killed by {signal.Signals(-code).name}"
        except ValueError:
            return f"killed by signal {-code}"
    return f"exit code {code}"


def judge(name: str, code: int | None, seconds: float, output: str,
          timeout: float | None = None) -> Result:
    """按退出码、输出与 XFAIL 清单给出判定；code 为 None 表示超时被杀。"""
    known = XFAIL.get(name)
    if code is None:
        return Result(name, "TIMEOUT", seconds, f"killed after {timeout:g}s", output)
    if code == 0:
        skip = next((line for line in output.splitlines() if line.startswith("SKIP:")), None)
        if skip is not None:
            return Result(name, "SKIP", seconds, skip[len("SKIP:"):].strip(), output)
        if known:
            return Result(name, "XPASS", seconds, f"listed in XFAIL (task {known['task']}) but passed",
                          output)
        return Result(name, "PASS", seconds, "", output)
    if known:
        return Result(name, "XFAIL", seconds,
                      f"known failure until task {known['task']}: {known['reason']}", output)
    return Result(name, "FAIL", seconds, _exit_note(code), output)


def run_one(path: Path, node: str | None, timeout: float) -> Result:
    why = missing_runtime(path, node)
    if why:
        return Result(path.name, "SKIP", 0.0, why)
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"       # 超时被杀时也保住已经打印的输出
    env["PYTHONIOENCODING"] = "utf-8"   # 与下面的解码一致
    # 输出写临时文件而不是管道：测试留下的孙进程握着输出端时也不会卡住读取。
    with tempfile.TemporaryFile() as sink:
        started = time.monotonic()
        try:
            proc = subprocess.Popen(command(path, node), cwd=REPO_ROOT, env=env, stdin=subprocess.DEVNULL,
                                    stdout=sink, stderr=subprocess.STDOUT, start_new_session=POSIX)
        except OSError as exc:
            return Result(path.name, "FAIL", 0.0, f"could not start: {exc}")
        try:
            code = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            code = None
            _stop_tree(proc)
            proc.wait()
        except BaseException:  # Ctrl+C 等：不把浏览器留在后台
            _stop_tree(proc)
            proc.wait()
            raise
        seconds = time.monotonic() - started
        leftovers = code is not None and _reap_leftovers(proc)
        sink.seek(0)
        output = sink.read().decode("utf-8", errors="replace")
    result = judge(path.name, code, seconds, output, timeout)
    if leftovers:
        result.note = "; ".join(filter(None, [result.note, "killed processes it left running"]))
    return result


def _tail(output: str, lines: int) -> list[str]:
    if lines <= 0:
        return []
    body = output.rstrip("\n").splitlines()
    return body[-lines:] if body else ["(no output)"]


def _last_line(output: str) -> str:
    return next((line.strip() for line in reversed(output.splitlines()) if line.strip()), "(no output)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="run_offline.py",
        description="Run each offline test under tests/ in its own subprocess and summarize the results.")
    parser.add_argument("-k", dest="pattern", metavar="SUBSTR",
                        help="only run tests whose file name contains SUBSTR (case-insensitive)")
    parser.add_argument("--list", action="store_true", help="list the selected tests and their commands, run nothing")
    parser.add_argument("--timeout", type=float, default=600.0, metavar="SECONDS",
                        help="per-test timeout in seconds (default: 600)")
    parser.add_argument("--tail", type=int, default=40, metavar="LINES",
                        help="output lines to show for FAIL / TIMEOUT (default: 40)")
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.tail < 0:
        parser.error("--tail must not be negative")
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if reconfigure:
        reconfigure(errors="replace")

    tests = discover(args.pattern)
    if not tests:
        print(f"no test under {TESTS_DIR} matches -k {args.pattern!r}")
        return 2
    node = find_node()
    width = max(len(p.name) for p in tests)

    if args.list:
        for path in tests:
            why = missing_runtime(path, node)
            marks = [f"[SKIP: {why}]"] if why else []
            if path.name in XFAIL:
                marks.append(f"[xfail until task {XFAIL[path.name]['task']}]")
            print(f"{path.name:<{width}}  {' '.join([shown(command(path, node)), *marks])}")
        print(f"{len(tests)} tests, cwd {REPO_ROOT}")
        return 0

    print(f"run_offline: {len(tests)} tests, timeout {args.timeout:g}s, cwd {REPO_ROOT}")
    print(f"python {sys.version.split()[0]} ({sys.executable}); "
          f"node {node or 'not found (.mjs tests will be SKIP)'}")
    results: list[Result] = []
    started = time.monotonic()
    interrupted = False
    digits = len(str(len(tests)))
    try:
        for i, path in enumerate(tests, 1):
            print(f"[{i:>{digits}}/{len(tests)}] {path.name} ... ", end="", flush=True)
            result = run_one(path, node, args.timeout)
            results.append(result)
            print(f"{result.verdict} ({result.seconds:.1f}s)" + (f"  {result.note}" if result.note else ""),
                  flush=True)
            if result.verdict in ("FAIL", "TIMEOUT"):
                for line in _tail(result.output, args.tail):
                    print(f"    | {line}")
            elif result.verdict == "XFAIL":
                print(f"    last line: {_last_line(result.output)}")
    except KeyboardInterrupt:
        interrupted = True
        print("\ninterrupted: the running test and its child processes were killed")
    total = time.monotonic() - started

    if results:
        print()
        print(f"{'RESULT':<8}{'TIME':>8}  {'TEST':<{width}}  NOTE")
        for r in results:
            print(f"{r.verdict:<8}{r.seconds:>7.1f}s  {r.name:<{width}}  {r.note}".rstrip())
    for r in results:
        if r.verdict == "XPASS":
            print(f"XPASS: {r.name} now passes; remove it from XFAIL in tests/run_offline.py")
    counts = {v: sum(r.verdict == v for r in results) for v in VERDICTS}
    print(f"SUMMARY: {counts['PASS']} passed, {counts['FAIL']} failed, {counts['TIMEOUT']} timeout, "
          f"{counts['XFAIL']} xfail, {counts['XPASS']} xpass, {counts['SKIP']} skipped "
          f"({len(results)} tests, {total:.1f}s)")
    if interrupted:
        return 130
    return 1 if any(r.verdict in BAD for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
