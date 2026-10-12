# Run by the target's /bin/sh through a non-TTY exec: $1 is the nonce, $2 the
# deadline in whole seconds, the rest the command. The command's own stdout
# and stderr pass through unchanged; this supervisor's records go to stderr
# through fd 3, each a whole line starting with the nonce: started <group>,
# expired (written by the timer when it fires), complete <exit code>, or
# nosleep (no timer possible; the command then replaces this shell). The
# shell's own job notices go to /dev/null.
nonce=$1 t=$2
shift 2
exec 3>&2 2>/dev/null
if ! command -v sleep >/dev/null; then
	printf '%s nosleep\n' "$nonce" >&3
	exec "$@" 2>&3 3>&-
fi
group=0
set -m && group=1
# No started record, no command: a record that cannot be written ends the
# supervisor before the command is launched.
printf '%s started %s\n' "$nonce" "$group" >&3 || exit 125
"$@" <&0 2>&3 3>&- &
pid=$!
# The timer's sleeps close fd 3: one left behind would hold the exec's
# stderr, and so the stream, open.
(
	# A closed output pipe must not end the timer before its kill.
	trap '' PIPE
	# The sleep is the timer's child: a TERM to the timer ends it too, also
	# where the shell has no job control to send it to the group.
	sleep "$t" 3>&- &
	s=$!
	trap 'kill "$s"; exit 0' TERM
	wait "$s"
	printf '\n%s expired\n' "$nonce" >&3
	kill -TERM -"$pid" || kill -TERM "$pid"
	sleep 2 3>&-
	kill -KILL -"$pid" || kill -KILL "$pid"
) </dev/null >/dev/null &
timer=$!
wait "$pid"
rc=$?
# Members of the command's group still holding its output would keep the
# exec stream open: they go with it.
[ "$group" -eq 1 ] && kill -KILL -"$pid"
kill -TERM -"$timer" || kill -TERM "$timer"
wait "$timer"
printf '\n%s complete %s\n' "$nonce" "$rc" >&3
