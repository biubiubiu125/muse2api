"""run_offline.py 自检：python3 tests/test_run_offline.py

在临时目录里造一套假测试，用 run_offline.main() 跑，核对每种判定（PASS / SKIP / FAIL /
TIMEOUT / XFAIL / XPASS、缺 node 的 .mjs）、EXTRA_ARGS 传参与 cwd、-k / --list / 无匹配时的
退出码，以及 POSIX 下超时先中断（测试的 finally 能跑完）再整组清理、残留子进程被清理。
不运行仓库里的真实测试，几秒内跑完。
"""
import contextlib
import io
import os
from pathlib import Path
import re
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_offline as ro  # noqa: E402

POSIX = os.name == "posix"
# 起一个睡 30s 的子进程并记下 pid，用来确认 run_offline 连同测试的子进程一起清理
SPAWN = ("import subprocess, sys\n"
         "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
         "open({pidfile!r}, 'w').write(str(child.pid))\n")
FAKE_TESTS = {
    "test_green_pass.py": "print('fine')\n",
    "test_green_skip.py": "print('setting up')\nprint('SKIP: widget not installed')\n",
    "test_green_known.py": "raise TypeError('still broken')\n",
    "test_green_args.py": ("import os, sys\n"
                           "assert sys.argv[1:] == ['engine.py', '--assert'], sys.argv\n"
                           "assert os.path.isfile('engine.py'), os.getcwd()\n"
                           "open('args.ran', 'w').close()\n"),
    "test_green_node.mjs": "console.log('needs node');\n",
    "test_red_fail.py": "print('about to fail')\nraise SystemExit(3)\n",
    "test_red_fixed.py": "print('fixed now')\n",
    # 不 flush：被停之前的输出要靠 run_offline 设置的 PYTHONUNBUFFERED 保住；
    # finally 用来确认超时先发 SIGINT，测试有机会跑完自己的清理
    "test_red_slow.py": SPAWN.format(pidfile="slow.pid") + ("import time\n"
                                                          "print('waiting')\n"
                                                          "try:\n"
                                                          "    time.sleep(30)\n"
                                                          "finally:\n"
                                                          "    open('slow.cleaned', 'w').close()\n"),
}
if POSIX:
    FAKE_TESTS["test_green_orphan.py"] = SPAWN.format(pidfile="orphan.pid") + "print('leaving a child behind')\n"
ROW = re.compile(r"^(PASS|FAIL|TIMEOUT|XFAIL|XPASS|SKIP) +\d+\.\ds  (test_\S+)", re.M)


def run(*argv):
    """进程内调用 run_offline.main()，返回（退出码、汇总表里的 {文件名: 判定}、全部输出）。"""
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = ro.main(list(argv))
    text = out.getvalue()
    return code, {name: verdict for verdict, name in ROW.findall(text)}, text


def gone(pid, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


def main():
    with tempfile.TemporaryDirectory(prefix="m2a-run-offline-") as tmp:
        root = Path(tmp)
        (root / "tests").mkdir()
        (root / "engine.py").write_text("", encoding="utf-8")
        for name, body in FAKE_TESTS.items():
            (root / "tests" / name).write_text(body, encoding="utf-8")
        ro.TESTS_DIR, ro.REPO_ROOT = root / "tests", root
        ro.EXTRA_ARGS = {"test_green_args.py": ["engine.py", "--assert"]}
        ro.XFAIL = {name: {"reason": "fixture reason", "task": "9.9"}
                    for name in ("test_green_known.py", "test_red_fixed.py")}
        ro.find_node = lambda: None

        code, rows, text = run("--list")
        assert (code, rows) == (0, {}), (code, rows, text)
        assert not (root / "args.ran").exists(), "--list must not run tests"
        for pattern in (r"^test_green_args\.py +python\S* tests/test_green_args\.py engine\.py --assert$",
                        r"^test_green_node\.mjs +node tests/test_green_node\.mjs \[SKIP: node not found\]$",
                        r"^test_red_fixed\.py +python\S* tests/test_red_fixed\.py \[xfail until task 9\.9\]$"):
            assert re.search(pattern, text, re.M), (pattern, text)
        print("PASS: --list prints each command with SKIP / xfail marks and runs nothing")

        code, rows, text = run("-k", "GREEN")
        want = {"test_green_args.py": "PASS", "test_green_known.py": "XFAIL", "test_green_node.mjs": "SKIP",
                "test_green_pass.py": "PASS", "test_green_skip.py": "SKIP"}
        if POSIX:
            want["test_green_orphan.py"] = "PASS"
        assert (code, rows) == (0, want), (code, rows, text)
        assert (root / "args.ran").exists(), "EXTRA_ARGS / cwd check did not run"
        for needle in ("widget not installed", "node not found", "known failure until task 9.9: fixture reason",
                       "last line: TypeError: still broken"):
            assert needle in text, (needle, text)
        assert re.search(rf"^SUMMARY: {3 if POSIX else 2} passed, 0 failed, 0 timeout, 1 xfail, 0 xpass, "
                         rf"2 skipped \({len(want)} tests, \d+\.\ds\)$", text, re.M), text
        if POSIX:
            assert "killed processes it left running" in text, text
            assert gone(int((root / "orphan.pid").read_text())), "leftover child of a passing test still running"
        print("PASS: -k selects case-insensitively; PASS / SKIP / XFAIL / missing node keep exit code 0"
              + ("; leftover child killed" if POSIX else ""))

        code, rows, text = run("-k", "red", "--timeout", "3")
        assert (code, rows) == (1, {"test_red_fail.py": "FAIL", "test_red_fixed.py": "XPASS",
                                    "test_red_slow.py": "TIMEOUT"}), (code, rows, text)
        for needle in ("exit code 3", "    | about to fail", "killed after 3s", "    | waiting",
                       "XPASS: test_red_fixed.py now passes; remove it from XFAIL"):
            assert needle in text, (needle, text)
        assert re.search(r"^SUMMARY: 0 passed, 1 failed, 1 timeout, 0 xfail, 1 xpass, 0 skipped \(3 tests, ",
                         text, re.M), text
        if POSIX:
            assert (root / "slow.cleaned").exists(), "timeout did not let the test run its finally cleanup"
            assert "KeyboardInterrupt" in text, text
            assert gone(int((root / "slow.pid").read_text())), "timed-out test's child still running"
        print("PASS: FAIL / TIMEOUT / XPASS give exit code 1 and print output tails"
              + ("; timeout interrupts first (cleanup ran), child stopped" if POSIX else ""))

        code, rows, text = run("-k", "no-such-test")
        assert (code, rows) == (2, {}), (code, rows, text)
        print("PASS: -k without matches exits 2")


if __name__ == "__main__":
    main()
    print("PASS: run_offline self-test")
