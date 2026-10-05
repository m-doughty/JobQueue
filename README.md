[![Actions Status](https://github.com/m-doughty/JobQueue/actions/workflows/test.yml/badge.svg)](https://github.com/m-doughty/JobQueue/actions)

NAME
====

JobQueue - dependency-aware job queues: lanes, priorities, dedup, cancellation and a cross-queue DAG

SYNOPSIS
========

```raku
use JobQueue;

class GreetJob does Job {
    has Str $.who is required;
}

# A gate Promise stands in for real work here; in production the
# runner talks to a backend and honours $job.cancelled as it goes.
my %gates;

my $queue = JobQueue::Queue.new(
    name            => 'greeter',
    is-parallel-now => -> { False },        # one at a time
    run-job         => -> $job {
        %gates{$job.id} = my $gate = Promise.new;
        start {
            await $gate;
            $job.cancelled ?? 'cancelled' !! do { say "hello, {$job.who}"; 'done' };
        }
    },
);

my $a = $queue.enqueue(GreetJob.new(scope-id => 'room-1', who => 'Ada'));
my $b = $queue.enqueue(GreetJob.new(scope-id => 'room-1', who => 'Grace'));

say $queue.running-count;      # 1 — serial mode
say $queue.pending-count;      # 1

%gates{$a.id}.keep;            # let the first job finish
await $a.completion;           # 'done'

$queue.cancel($b.id);          # cancelled before it ever ran
say $b.state;                  # cancelled
```

DESCRIPTION
===========

`JobQueue` is the scheduling nucleus extracted from a production TUI chat client, where "just `start { }` it" collapsed under contact with reality: a backend that allows one request at a time, a UI that has to render "queued (#3 of 5)", a user who cancels mid-stream, and a turn made of five interdependent inference calls across two different backends.

It gives you four pieces.

`Job` — the unit of work
------------------------

A role you compose into your own class. It carries the immutable inputs captured at enqueue time, a cancel Promise, a completion Promise, and the scheduling policy (`lane`, `priority`, `dedup-key`, `depends-on`, `after`). Its three state transitions — `finish`, `request-cancel`, `begin-running` — are atomic and return True only to the caller that performed them, which is how exactly one side ends up owning the terminal event.

`JobQueue::Queue` — one resource, one queue
-------------------------------------------

Serial or parallel (re-evaluated per dispatch, so a config flip picks up at the next free slot), with a `max-concurrent` cap, lane serialisation, priority dispatch and live-job dedup. Cancellation is cooperative: the queue keeps the token and calls your `on-cancel`; your runner unwinds.

`JobQueue::Coordinator` — the DAG
---------------------------------

A registry of named queues plus a dependency graph across them. Declare the chain flat instead of nesting `.then` callbacks:

```raku
my $coord = JobQueue::Coordinator.new;
$coord.register-queue('text',  $text-queue);
$coord.register-queue('image', $image-queue);

my $summary = $coord.submit('text', SummaryJob.new(scope-id => $doc));
$coord.submit('text',  TagJob.new(  scope-id => $doc, depends-on => [$summary.id]));
$coord.submit('image', CoverJob.new(scope-id => $doc, depends-on => [$summary.id]));
$coord.submit('text',  AuditJob.new(scope-id => $doc, after      => [$summary.id]));

$coord.tick;    # per frame: release, supersede, pump, prune
```

Hard edges (`depends-on`) require their dependency to finish `done`, and a failure cascades transitively as `superseded`. Soft edges (`after`) only wait for a terminal state and run regardless of the outcome. Each dependency's `state` and `result` land in the dependent's `dep-results` at release. Completed predecessors may be pruned from live bookkeeping, but the coordinator keeps a bounded terminal-outcome cache so a dependent submitted shortly after pruning still sees the predecessor's `state`, `result` and `dep-extras`. Every dependency resolution also records a compact `dependency-state` report on the job: `ready`, `blocked` or `doomed`, plus the first relevant prerequisite id, edge, observed state and required state for non-ready jobs.

`JobQueue::FailedRegistry` — retry with identical inputs
--------------------------------------------------------

A capped, insertion-ordered map of failed and user-cancelled jobs, with an atomic `claim` so two retry clicks can't both win.

GUARANTEES
==========

These are not incidental behaviours; they are the contracts the implementation exists to hold, each one paid for by a production bug.

  * **Exactly one terminal event per job.** A job that ever started gets its terminal from the runner's then-hook at true drain end; a job cancelled while pending gets it inside `cancel`. A cancel racing a runner's own completion cannot double-emit.

  * **Terminal routing keys on the kept completion value, never on `$job.state`.** The kept value is immutable; `state` loses races by design. So a runner that self-finishes `'cancelled'` emits `queue/job-cancelled`, not a red `queue/job-failed`.

  * **A cancelled running job announces, then drains.** Cancel emits the **non-terminal** `queue/job-cancelling` (state and phase pinned to `cancelling`, `cancellable` False) **before** calling `on-cancel`, and defers the terminal until the runner unwinds. The ledger honestly reads "cancelling" for exactly as long as the job still occupies the backend. Dropping the row early made the queue look empty while it was still blocked.

  * **Dedup is checked against LIVE jobs only.** Cancel and supersede keep the completion Promise synchronously, so a cancelled-but-still- draining job never absorbs a fresh enqueue — the rapid A→B→A case.

  * **`is-parallel-now` is evaluated outside the lock, and never on an idle tick.** `tick` early-outs on an O(1) pending check, because production policy closures read a database and per-frame callers cannot pay for that.

  * **Lanes count live jobs; the serial gate and cap count all of them.** A cancelled job still draining releases its lane immediately (its successor may queue up) but still occupies the physical backend slot (nothing new starts in serial mode).

  * **Priority is per freed slot, FIFO within a level, and lane-blocked jobs are skipped rather than blocking.** A zero-priority job in a free lane overtakes a lane-blocked prioritised one; the alternative is a stalled queue.

  * **`note-phase` / `note-progress` are guarded.** Terminal, cancelled, or not-currently-running jobs are ignored, so a late note from a straggling worker can never resurrect a finished row.

  * **Pruned terminal dependency outcomes remain briefly addressable.** The coordinator retains a bounded FIFO cache of terminal `dep-results` payloads, so asynchronous DAG extension can attach a dependent after its predecessor Job object was pruned without misclassifying that predecessor as unknown. The cache stores the same top-level handoff Hash shape the dependent receives: `state`, `result` and filtered `dep-extras`. The coordinator itself adds no Job object, Promise or Lock to that handoff; values supplied by `result` and `dep-extras` keep their normal Raku reference semantics. When the cache evicts, hard and soft edges use the normal unknown-dependency rules.

  * **A misbehaving runner cannot stall the queue.** A runner returning a non-Promise is coerced to a kept one; a runner that throws has its exception message captured onto `$job.error` and becomes a normal failure terminal.

  * **`live-count` and `total-count` are different questions.** Lock a reply box on the former (what the user waits for); size a backend pool with the latter (what physically occupies it). `physical-job-ids` returns the atomic id snapshot behind the physical view, including cancelled/superseded runners until their drain actually ends.

THE STORE SINK CONTRACT
=======================

Both the queue and the coordinator take an optional `store`. The entire interface is one method:

```raku
method dispatch(Str:D $event, *%payload) { ... }
```

and the queue always calls it in exactly one shape:

```raku
$store.dispatch($event, queue => $queue-name, job => %snapshot);
```

`%snapshot` comes from `JobQueue::Queue.job-snapshot`: plain scalars only — id, scope-id, state, timestamps, lane, priority, phase, `error`, the coordinator's compact `dependency-state` report when present, plus whatever your class's `snapshot-extras` adds. `dependency-state` is queue-owned, so `snapshot-extras` cannot override it. It is safe to drop into a Redux-style store, serialise, or send over a socket. The full Job object is not: it holds Promises, closures and a Lock.

`$.store` is untyped so any object answering `dispatch` works. `JobQueue::EventSink` names the contract if you want the compile-time check:

```raku
class LedgerSink does JobQueue::EventSink {
    has %.rows;
    method dispatch(Str:D $event, *%payload) {
        %!rows{%payload<job><id>} = %( :$event, |%payload<job> );
    }
}
```

Leaving `store` undefined disables the event stream entirely.

OBSERVABILITY
=============

The queue and coordinator log structured events and open tracing spans, but they bring no logging framework with them. Inject `log` and `tracer` objects; both default to silent null objects. See `JobQueue::Observability` for the (very small) contracts and adapter examples.

```raku
my $queue = JobQueue::Queue.new(
    :$name, :&is-parallel-now, :&run-job,
    log    => MyLogAdapter.new,
    tracer => MySpanTracer.new,
);
```

EXTENDING A JOB
===============

Two override points let your job class contribute payload without the queue ever learning your class exists:

```raku
class RenderJob does Job {
    has Int $.message-id is rw;
    has Str $.template is required;

    # merged into the store-event snapshot
    method snapshot-extras(--> Hash) {
        my %e = template => $!template;
        %e<message-id> = $!message-id if $!message-id.defined;
        %e;
    }

    # merged into dependents' dep-results{ this-job-id }
    method dep-extras(--> Hash) {
        $!message-id.defined ?? %( message-id => $!message-id ) !! %();
    }
}
```

Queue-owned keys always win over extras, so a job cannot make its own ledger row lie about its state. Override with the **exact** signature shown — Raku will accept a different one and quietly leave the role's version in charge.

MODULES
=======

  * `JobQueue::Job` — the `Job` role and `keep-once`.

  * `JobQueue::Queue` — the queue.

  * `JobQueue::Coordinator` — named queues + the dependency DAG.

  * `JobQueue::FailedRegistry` — retry bookkeeping.

  * `JobQueue::EventSink` — the store-sink documentation role.

  * `JobQueue::Observability` — null log/tracer and their contracts.

`use JobQueue;` loads all of them and exports the `Job` role and the `keep-once` sub; the classes are reachable by their full names.

AUTHOR
======

Matt Doughty <matt@apogee.guru>

COPYRIGHT AND LICENSE
=====================

Copyright 2026 Matt Doughty

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

### sub EXPORT

```raku
sub EXPORT(
    |
) returns Mu
```

Re-export the lexical symbols of the sub-modules so `use JobQueue` is a complete entry point. The classes (JobQueue::Queue and friends) are global package names and are already visible from the `use` statements above; only Job and keep-once are lexical exports that need forwarding.

