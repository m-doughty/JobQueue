=begin pod

=head1 NAME

JobQueue::EventSink - the one method a queue's store sink has to answer

=head1 DESCRIPTION

C<JobQueue::Queue> and C<JobQueue::Coordinator> take an optional
C<store> object and broadcast the job lifecycle to it. The entire
contract is one method:

=begin code :lang<raku>

method dispatch(Str:D $event, *%payload) { ... }

=end code

The queue always calls it in exactly this shape:

=begin code :lang<raku>

$store.dispatch($event, queue => $queue-name, job => %snapshot);

=end code

C<%snapshot> is the Hash from C<JobQueue::Queue.job-snapshot> — plain
scalars only, safe to drop into a Redux-style store, serialise, or
push over a socket. No Promises, no closures, no live Job object.

=head2 Why the attribute is untyped

C<$.store> is deliberately B<not> constrained to this role. Sinks are
usually pre-existing objects — a C<Selkie::Store>, a test double, a
Cro websocket wrapper — that already answer C<dispatch> and shouldn't
have to grow a C<does> clause to be usable here. The role exists so
that new sinks can opt into a compile-time check and so the contract
has a name to point at.

=head2 Example

=begin code :lang<raku>

use JobQueue::EventSink;

class LedgerSink does JobQueue::EventSink {
    has @.rows;
    method dispatch(Str:D $event, *%payload) {
        @!rows.push: %( :$event, queue => %payload<queue>,
                        id => %payload<job><id>,
                        state => %payload<job><state> );
    }
}

my $queue = JobQueue::Queue.new(
    name => 'text-backend', store => LedgerSink.new,
    is-parallel-now => -> { False },
    run-job => -> $job { start { $job.finish('done'); 'done' } },
);

=end code

=head2 The event vocabulary

=item C<queue/job-enqueued> — accepted onto the queue (not started).
=item C<queue/job-waiting> — held by the coordinator on a dependency.
=item C<queue/job-started> — the runner was spawned.
=item C<queue/job-phase-changed> — C<note-phase> on a running job.
=item C<queue/job-progressed> — C<note-progress>; adds C<progress => { value, max, node }>.
=item C<queue/job-cancelling> — B<non-terminal>: a running job is draining.
=item C<queue/job-finished> — terminal, outcome C<done>.
=item C<queue/job-failed> — terminal, any other failure outcome.
=item C<queue/job-cancelled> — terminal, user intent.
=item C<queue/job-superseded> — terminal, system replacement.

Exactly one of the four terminal events fires per job, ever.

=end pod

unit role JobQueue::EventSink;

#|( Receive one lifecycle event. C<$event> is one of the
    C<queue/job-*> strings; C<%payload> always carries C<queue> (the
    queue name) and C<job> (the snapshot Hash). Implementations must
    not throw — the queue dispatches from its runner then-hooks, and
    a throwing sink would surface as an unobserved broken Promise. )
method dispatch(Str:D $event, *%payload) { ... }
