=begin pod

=head1 NAME

JobQueue::FailedRegistry - capped, insertion-ordered registry of retryable jobs

=head1 DESCRIPTION

When a job fails or the user cancels it, the job object is the only
place its exact inputs still exist: the captured parameters, the
target ids, the snapshot of context taken at enqueue time. Throw it
away and "retry" degrades into "run something vaguely similar with
whatever the world looks like now".

C<JobQueue::FailedRegistry> keeps the last C<cap> such jobs (default
20, oldest evicted first) so a retry can rebuild an identical job.

=head1 SYNOPSIS

=begin code :lang<raku>

use JobQueue::FailedRegistry;

has JobQueue::FailedRegistry $.failed .= new;

# ... in the terminal handler:
$!failed.remember($job)            if $outcome eq 'error'|'broken';
$!failed.remember-cancelled($job)  if $outcome eq 'cancelled';

# ... in the retry path:
method retry-job(Str:D $job-id --> Bool) {
    my $old = $!failed.claim($job-id);   # atomic: exactly one winner
    return False without $old;
    $queue.enqueue(RenderJob.new(
        scope-id => $old.scope-id,
        params   => $old.params,         # bit-identical inputs
    ));
    True;
}

=end code

=head2 Why C<claim> and not "look up, then delete"

C<claim> is a destructive, lock-guarded remove-and-return: it is the
race gate. Two retry clicks on the same row, or a retry racing a
worker that is writing a fresh failure, resolve to exactly one winner
— the loser gets C<Nil> and does nothing. A read-then-delete pair
double-submits under exactly the load where the user is most likely
to click twice.

Use C<peek> when you only want to render a row ("this job is
retryable"), never to decide whether to act.

=head2 Cancelled jobs are retryable, superseded ones are not

C<remember-cancelled> encodes a UX rule worth stating out loud: a job
the B<user> cancelled stays actionable — they explicitly acted on it,
and "undo my cancel" is a real intent. A job the B<system> superseded
does not: its replacement is already scheduled, or it was never
wanted. Passing a superseded job to C<remember-cancelled> is a
silent, deliberate no-op returning False, so cancel handlers can pass
whatever the queue handed them without pre-filtering.

The check reads the job's kept C<completion> value (the immutable
outcome the queue routes on) and falls back to C<state> for a job
that hasn't settled its completion Promise.

=head2 Thread safety

Every operation takes the registry's own C<Lock>. Registries are
written from runner threads and terminal then-hooks, and read from
whatever thread renders your UI — the same crash class as any other
shared mutable map.

=end pod

unit class JobQueue::FailedRegistry;

#|( How many jobs to keep. Once exceeded, the oldest remembered job
    is evicted. Must be at least 1 — a zero cap would accept a job
    and immediately drop it, which is worse than refusing to exist. )
has Int $.cap = 20;

has      %!jobs;        # job-id => job
has Str  @!order;       # insertion order, oldest first
has Lock $!lock = Lock.new;

submethod TWEAK() {
    die "JobQueue::FailedRegistry: cap must be at least 1, got $!cap"
        if $!cap < 1;
}

#|( Remember C<$job> for later retry. Idempotent: a job whose id is
    already present is left exactly where it is (its original
    insertion position is preserved, so re-remembering does not
    refresh its place in the eviction order) and False is returned.

    Returns True iff this call inserted the job. An undefined job is
    a no-op returning False, so callers can pass an optional job
    straight through. )
method remember($job --> Bool) {
    return False without $job;
    $!lock.protect: {
        return False if %!jobs{$job.id}:exists;
        %!jobs{$job.id} = $job;
        @!order.push: $job.id;
        while @!order.elems > $!cap {
            %!jobs{@!order.shift}:delete;
        }
        True;
    }
}

#|( Remember a user-cancelled job, skipping system supersedes. See
    the "cancelled jobs are retryable" section above. Returns True
    iff the job was inserted. )
method remember-cancelled($job --> Bool) {
    return False without $job;
    my Str $outcome = $job.completion.status ~~ Kept
        ?? ($job.completion.result // '').Str
        !! ($job.state // '');
    return False if $outcome eq 'superseded';
    self.remember($job);
}

#|( Atomically remove and return the job registered under C<$job-id>,
    or Nil if it isn't there (evicted, never remembered, or already
    claimed). This is the retry gate: concurrent claims of the same
    id produce exactly one non-Nil result. )
method claim(Str:D $job-id) {
    $!lock.protect: {
        my $job = %!jobs{$job-id};
        with $job {
            %!jobs{$job-id}:delete;
            @!order = @!order.grep(* ne $job-id).Array;
        }
        $job // Nil;
    }
}

#|( Non-destructive lookup — the job under C<$job-id>, or Nil.
    For rendering only: deciding to act on the result of a C<peek> is
    the race C<claim> exists to close. )
method peek(Str:D $job-id) {
    $!lock.protect: { %!jobs{$job-id} // Nil }
}

#| Remembered job ids, oldest first. Snapshot; safe to iterate.
method ids(--> List) {
    $!lock.protect: { @!order.List }
}

#| How many jobs are currently remembered.
method elems(--> Int) {
    $!lock.protect: { %!jobs.elems }
}
