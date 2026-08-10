=begin pod

=head1 NAME

JobQueue - dependency-aware job queues: lanes, priorities, dedup, cancellation and a cross-queue DAG

=head1 SYNOPSIS

=begin code :lang<raku>

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

=end code

=head1 DESCRIPTION

C<JobQueue> is the scheduling nucleus extracted from a production TUI
chat client, where "just C<start { }> it" collapsed under contact
with reality: a backend that allows one request at a time, a UI that
has to render "queued (#3 of 5)", a user who cancels mid-stream, and
a turn made of five interdependent inference calls across two
different backends.

It gives you four pieces.

=head2 C<Job> — the unit of work

A role you compose into your own class. It carries the immutable
inputs captured at enqueue time, a cancel Promise, a completion
Promise, and the scheduling policy (C<lane>, C<priority>,
C<dedup-key>, C<depends-on>, C<after>). Its three state transitions —
C<finish>, C<request-cancel>, C<begin-running> — are atomic and
return True only to the caller that performed them, which is how
exactly one side ends up owning the terminal event.

=head2 C<JobQueue::Queue> — one resource, one queue

Serial or parallel (re-evaluated per dispatch, so a config flip picks
up at the next free slot), with a C<max-concurrent> cap, lane
serialisation, priority dispatch and live-job dedup. Cancellation is
cooperative: the queue keeps the token and calls your C<on-cancel>;
your runner unwinds.

=head2 C<JobQueue::Coordinator> — the DAG

A registry of named queues plus a dependency graph across them.
Declare the chain flat instead of nesting C<.then> callbacks:

=begin code :lang<raku>

my $coord = JobQueue::Coordinator.new;
$coord.register-queue('text',  $text-queue);
$coord.register-queue('image', $image-queue);

my $summary = $coord.submit('text', SummaryJob.new(scope-id => $doc));
$coord.submit('text',  TagJob.new(  scope-id => $doc, depends-on => [$summary.id]));
$coord.submit('image', CoverJob.new(scope-id => $doc, depends-on => [$summary.id]));
$coord.submit('text',  AuditJob.new(scope-id => $doc, after      => [$summary.id]));

$coord.tick;    # per frame: release, supersede, pump, prune

=end code

Hard edges (C<depends-on>) require their dependency to finish
C<done>, and a failure cascades transitively as C<superseded>. Soft
edges (C<after>) only wait for a terminal state and run regardless of
the outcome. Each dependency's C<state> and C<result> land in the
dependent's C<dep-results> at release.

=head2 C<JobQueue::FailedRegistry> — retry with identical inputs

A capped, insertion-ordered map of failed and user-cancelled jobs,
with an atomic C<claim> so two retry clicks can't both win.

=head1 GUARANTEES

These are not incidental behaviours; they are the contracts the
implementation exists to hold, each one paid for by a production bug.

=item B<Exactly one terminal event per job.> A job that ever started
  gets its terminal from the runner's then-hook at true drain end; a
  job cancelled while pending gets it inside C<cancel>. A cancel
  racing a runner's own completion cannot double-emit.

=item B<Terminal routing keys on the kept completion value, never on
  C<$job.state>.> The kept value is immutable; C<state> loses races by
  design. So a runner that self-finishes C<'cancelled'> emits
  C<queue/job-cancelled>, not a red C<queue/job-failed>.

=item B<A cancelled running job announces, then drains.> Cancel emits
  the B<non-terminal> C<queue/job-cancelling> (state and phase pinned
  to C<cancelling>, C<cancellable> False) B<before> calling
  C<on-cancel>, and defers the terminal until the runner unwinds. The
  ledger honestly reads "cancelling" for exactly as long as the job
  still occupies the backend. Dropping the row early made the queue
  look empty while it was still blocked.

=item B<Dedup is checked against LIVE jobs only.> Cancel and supersede
  keep the completion Promise synchronously, so a cancelled-but-still-
  draining job never absorbs a fresh enqueue — the rapid A→B→A case.

=item B<C<is-parallel-now> is evaluated outside the lock, and never on
  an idle tick.> C<tick> early-outs on an O(1) pending check, because
  production policy closures read a database and per-frame callers
  cannot pay for that.

=item B<Lanes count live jobs; the serial gate and cap count all of
  them.> A cancelled job still draining releases its lane immediately
  (its successor may queue up) but still occupies the physical
  backend slot (nothing new starts in serial mode).

=item B<Priority is per freed slot, FIFO within a level, and
  lane-blocked jobs are skipped rather than blocking.> A zero-priority
  job in a free lane overtakes a lane-blocked prioritised one; the
  alternative is a stalled queue.

=item B<C<note-phase> / C<note-progress> are guarded.> Terminal,
  cancelled, or not-currently-running jobs are ignored, so a late note
  from a straggling worker can never resurrect a finished row.

=item B<A misbehaving runner cannot stall the queue.> A runner
  returning a non-Promise is coerced to a kept one; a runner that
  throws has its exception message captured onto C<$job.error> and
  becomes a normal failure terminal.

=item B<C<live-count> and C<total-count> are different questions.>
  Lock a reply box on the former (what the user waits for); size a
  backend pool with the latter (what physically occupies it).

=head1 THE STORE SINK CONTRACT

Both the queue and the coordinator take an optional C<store>. The
entire interface is one method:

=begin code :lang<raku>

method dispatch(Str:D $event, *%payload) { ... }

=end code

and the queue always calls it in exactly one shape:

=begin code :lang<raku>

$store.dispatch($event, queue => $queue-name, job => %snapshot);

=end code

C<%snapshot> comes from C<JobQueue::Queue.job-snapshot>: plain scalars
only — id, scope-id, state, timestamps, lane, priority, phase,
C<error>, plus whatever your class's C<snapshot-extras> adds. It is
safe to drop into a Redux-style store, serialise, or send over a
socket. The full Job object is not: it holds Promises, closures and a
Lock.

C<$.store> is untyped so any object answering C<dispatch> works.
C<JobQueue::EventSink> names the contract if you want the
compile-time check:

=begin code :lang<raku>

class LedgerSink does JobQueue::EventSink {
    has %.rows;
    method dispatch(Str:D $event, *%payload) {
        %!rows{%payload<job><id>} = %( :$event, |%payload<job> );
    }
}

=end code

Leaving C<store> undefined disables the event stream entirely.

=head1 OBSERVABILITY

The queue and coordinator log structured events and open tracing
spans, but they bring no logging framework with them. Inject C<log>
and C<tracer> objects; both default to silent null objects. See
C<JobQueue::Observability> for the (very small) contracts and adapter
examples.

=begin code :lang<raku>

my $queue = JobQueue::Queue.new(
    :$name, :&is-parallel-now, :&run-job,
    log    => MyLogAdapter.new,
    tracer => MySpanTracer.new,
);

=end code

=head1 EXTENDING A JOB

Two override points let your job class contribute payload without the
queue ever learning your class exists:

=begin code :lang<raku>

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

=end code

Queue-owned keys always win over extras, so a job cannot make its own
ledger row lie about its state. Override with the B<exact> signature
shown — Raku will accept a different one and quietly leave the role's
version in charge.

=head1 MODULES

=item C<JobQueue::Job> — the C<Job> role and C<keep-once>.
=item C<JobQueue::Queue> — the queue.
=item C<JobQueue::Coordinator> — named queues + the dependency DAG.
=item C<JobQueue::FailedRegistry> — retry bookkeeping.
=item C<JobQueue::EventSink> — the store-sink documentation role.
=item C<JobQueue::Observability> — null log/tracer and their contracts.

C<use JobQueue;> loads all of them and exports the C<Job> role and the
C<keep-once> sub; the classes are reachable by their full names.

=head1 AUTHOR

Matt Doughty <matt@apogee.guru>

=head1 COPYRIGHT AND LICENSE

Copyright 2026 Matt Doughty

This library is free software; you can redistribute it and/or modify
it under the Artistic License 2.0.

=end pod

use JobQueue::Job;
use JobQueue::Queue;
use JobQueue::Coordinator;
use JobQueue::FailedRegistry;
use JobQueue::EventSink;
use JobQueue::Observability;

module JobQueue { }

#|( Re-export the lexical symbols of the sub-modules so `use JobQueue`
    is a complete entry point. The classes (JobQueue::Queue and
    friends) are global package names and are already visible from the
    `use` statements above; only Job and keep-once are lexical exports
    that need forwarding. )
sub EXPORT(|) {
    Map.new(
        'Job'        => JobQueue::Job::Job,
        '&keep-once' => &keep-once,
        'NullLog'    => JobQueue::Observability::NullLog,
        'NullSpan'   => JobQueue::Observability::NullSpan,
        'NullTracer' => JobQueue::Observability::NullTracer,
    )
}
