# ADR-0019: A consumer that stops on its own takes the process down

Status: Accepted (2026-10-25)

Respects the real Postgres and Kafka runtime. No wire change, no migration, no
new env var.

## Context

`main.go` started every Kafka consumer in a goroutine and, when one returned an
error, only logged `consumer stopped`. The HTTP server kept running, `/healthz`
and `/readyz` kept answering 200, and the read model behind that consumer froze.

This happened live. Under heavy host load the broker reset the connections
(`connection reset by peer`) and answered `Group Load In Progress`; the
pack-level consumer stopped and never came back. The pod stayed Ready for hours
while its consumer lagged by 149 messages, and every transfer proposal went out
unrounded because the case-multiple view (ADR-0017) never learned a new pack
level. Nothing alerted, because nothing was unhealthy.

## Decision

A consumer that stops while the service is still meant to run ends the process.

1. `startRunners` starts every consumer and reports the FIRST unexpected stop on
   a channel; `run()` returns that error, so the process exits non-zero and the
   kubelet restarts the pod. The consumers rejoin their groups and resume from
   the committed offset.
2. "Unexpected" means: the consumer returned an error that is not
   `context.Canceled`, or returned nil, while the service's context was still
   live. A stop after the context ended (a requested shutdown) is clean and
   reports nothing; this is the same distinction the network-fulfillment
   death-watcher makes.
3. The channel holds one death. Further deaths are logged and dropped so a
   second dying consumer never blocks on a send.

In-process retry is deliberately not added: a restart rejoins every group with
fresh connections and is the repair the fleet already relies on, and an
in-process loop would need its own backoff, jitter and give-up rule.

## Consequences

- A transient broker problem now shows as a pod restart (visible in
  `kubectl get pods` and the restart-count alerts) instead of a silent stale
  read model.
- After the restart the group resumes from its committed offset: live, the
  stalled group went from a lag of 149 to 0 within seconds of the pod coming up.
- If the broker is down for a long time the pod crash-loops with backoff, which
  is the accurate state of a service that cannot consume.
