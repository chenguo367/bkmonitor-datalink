# Executed with the declared Python 3 interpreter via Kubernetes non-TTY exec.
# Child output is data only; control records are emitted by this supervisor.
import base64
import json
import os
import selectors
import signal
import subprocess
import sys
import threading
import time


def main():
    nonce, timeout_text, budget_text, command_text = sys.argv[1:5]
    timeout = float(timeout_text)
    budget = int(budget_text)
    argv = json.loads(command_text)
    connected = [True]

    def emit(event, **fields):
        if not connected[0]:
            return
        fields.update(nonce=nonce, event=event)
        try:
            sys.stdout.write(json.dumps(fields, separators=(",", ":")) + "\n")
            sys.stdout.flush()
        except (BrokenPipeError, OSError):
            # Closing an exec connection does not cancel the target timer.
            # Continue supervising even when the terminal receipt cannot be sent.
            connected[0] = False
            null_fd = os.open(os.devnull, os.O_WRONLY)
            os.dup2(null_fd, sys.stdout.fileno())
            os.close(null_fd)

    # An unsupported runtime fails before starting the requested argv.
    if os.name != "posix" or not hasattr(os, "killpg"):
        emit("unsupported")
        return
    try:
        child = subprocess.Popen(
            argv, stdin=sys.stdin.buffer, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, start_new_session=True,
        )
    except (OSError, ValueError):
        emit("launch_failed")
        return

    stopped = threading.Event()
    expired = threading.Event()

    def group_signal(sig):
        try:
            os.killpg(child.pid, sig)
        except ProcessLookupError:
            pass

    # The deadline must remain active even if writing an output/control frame
    # blocks on a connected but stalled exec pipe.
    def enforce_deadline():
        if stopped.wait(timeout):
            return
        expired.set()
        group_signal(signal.SIGTERM)
        try:
            child.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            pass
        group_signal(signal.SIGKILL)
        child.wait()

    threading.Thread(target=enforce_deadline, daemon=True).start()
    emit("started")
    selector = selectors.DefaultSelector()
    selector.register(child.stdout, selectors.EVENT_READ, "stdout")
    selector.register(child.stderr, selectors.EVENT_READ, "stderr")
    term_at = None
    truncated = False

    while selector.get_map() or child.poll() is None:
        now = time.monotonic()
        if term_at is None and expired.is_set():
            term_at = now
        if term_at is not None and now >= term_at + 2.5:
            break
        for key, _ in selector.select(0.02):
            data = os.read(key.fileobj.fileno(), 16384)
            if not data:
                selector.unregister(key.fileobj)
                key.fileobj.close()
                continue
            kept = data[:budget]
            budget -= len(kept)
            truncated = truncated or len(kept) != len(data)
            if kept:
                emit("output", stream=key.data,
                     data=base64.b64encode(kept).decode("ascii"))
    selector.close()
    child.stdout.close()
    child.stderr.close()
    # Stop remaining members of the command group even after its leader exits.
    # Deliberately detached descendants are outside the declared group contract.
    group_signal(signal.SIGKILL)
    try:
        exit_code = child.wait(timeout=0.5)
    except subprocess.TimeoutExpired:
        emit("unknown", truncated=truncated)
        return
    stopped.set()
    emit("complete", exit_code=exit_code,
         timed_out=term_at is not None or expired.is_set(),
         truncated=truncated)


main()
