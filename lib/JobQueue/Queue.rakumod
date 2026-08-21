=begin pod

=head1 NAME

JobQueue::Queue - per-resource FIFO job queue with lanes, priorities and cancellation

=head1 DESCRIPTION

A single C<JobQueue::Queue> serialises (or parallelises) jobs against
one logical resource: an LLM backend, a GPU render server, an
outbound API with a rate limit. Run one queue per resource and route
jobs to whichever one applies.

The queue itself is policy-free. Callers supply two closures:

=item C<is-parallel-now> — re-evaluated on every dispatch attempt, so
  a config change picks up on the next free slot. Evaluated B<outside>
  the queue lock, because in practice it reads a database.
=item C<run-job> — invoked with the chosen job; must return a
  C<Promise> that resolves when the job exits, kept or broken. (A
  runner that returns something else is coerced to a kept Promise
  rather than being allowed to stall the queue forever.)

=head1 SYNOPSIS

=begin code :lang<raku>

use JobQueue::Job;
use JobQueue::Queue;

class EchoJob does Job { has Str $.text is required }

my %config = mode => 'parallel';

my $queue = JobQueue::Queue.new(
    name            => 'text-backend',
    max-concurrent  => 4,
    is-parallel-now => -> { %config<mode> eq 'parallel' },
    run-job         => -> $job {
        start {
            for ^10 {
                last if $job.cancelled;      # cooperative cancellation
                say $job.text;
            }
            $job.cancelled ?? 'cancelled' !! 'done';
        }
    },
);

my $job = $queue.enqueue(EchoJob.new(scope-id => 'room-1', text => 'hi'));
await $job.completion;

=end code

Call C<tick> from your frame loop / event loop; it is a single
integer compare when nothing is pending.

=head2 Concurrency model

When C<is-parallel-now> returns False, exactly one job runs at a
time; new enqueues wait in the pending list until the running job's
Promise resolves. When it returns True, the queue drains the pending
list up to C<max-concurrent> (0 = unbounded).

Switching modes mid-queue is supported: a parallel queue going serial
lets its in-flight jobs finish naturally and starts nothing new while
the queue is non-empty; serial going parallel drains at the next
C<tick> or C<enqueue>.

The serial gate and the C<max-concurrent> cap count B<every> running
job, including cancelled ones still draining — a job whose runner
hasn't unwound still physically occupies the backend.

=head2 Lanes

Jobs sharing a non-empty C<lane> run at most one at a time, FIFO
within the lane, B<even in parallel mode>. Jobs in other lanes
overtake freely.

=begin code :lang<raku>

# Two documents render concurrently; each document's own jobs serialise.
$queue.enqueue(RenderJob.new(scope-id => 'doc-1', lane => 'render:doc-1'));
$queue.enqueue(RenderJob.new(scope-id => 'doc-1', lane => 'render:doc-1'));
$queue.enqueue(RenderJob.new(scope-id => 'doc-2', lane => 'render:doc-2'));

=end code

Lane occupancy counts only B<live> jobs: a cancelled job still
draining unabortable work releases its lane immediately, so its
successor can queue up behind the physical drain rather than behind
the logical job.

=head2 Priorities

Each freed slot goes to the highest C<priority> among lane-B<eligible>
pending jobs, FIFO within a level. Lane-blocked jobs are skipped, not
blocking: a zero-priority job in a free lane overtakes a lane-blocked
prioritised one, because the alternative is a stalled queue.

=head2 Dedup

A job with a non-empty C<dedup-key> is dropped at C<enqueue> if a
B<live> job (completion not Kept) already carries that key — the
caller gets the existing job back:

=begin code :lang<raku>

my $a = $queue.enqueue(Refresh.new(scope-id => 1, dedup-key => 'refresh:1'));
my $b = $queue.enqueue(Refresh.new(scope-id => 1, dedup-key => 'refresh:1'));
say $a.id eq $b.id;   # True — nothing extra was enqueued

=end code

"Live" is the load-bearing word. Cancel and supersede keep the
completion Promise B<synchronously>, so a cancelled-but-still-draining
job never absorbs a fresh enqueue — the rapid A→B→A case.

=head2 Cancellation

C<cancel($job-id)> on a B<pending> job drops it from the queue and
emits its terminal event immediately: nothing ever ran.

C<cancel($job-id)> on a B<running> job keeps the job's cancel token,
finishes the job (so the eventual runner return can't overwrite the
terminal state), calls your C<on-cancel> so you can fire backend-side
aborts — and then B<defers the terminal event> until the runner
actually unwinds. What fires immediately is the non-terminal
C<queue/job-cancelling>, with the snapshot pinned to state and phase
C<cancelling> and C<cancellable> False.

That deferral is deliberate scar tissue. Emitting the terminal at
cancel time makes a ledger drop the row while the drain still owns
the backend: the queue looks empty yet refuses to start anything. The
corollary is that a runner which never resolves leaves the row
C<cancelling> forever — bound your runners with watchdogs.

C<:outcome> defaults to C<'cancelled'> (user intent); pass
C<'superseded'> for system-initiated replacement.

=begin code :lang<raku>

$queue.cancel($job.id, on-cancel => -> $j {
    $j.backend.abort($j.request-handle);   # optional, may be a no-op
});

# "Abort whatever I just started in this conversation":
$queue.cancel-most-recent-in-scope($conversation-id, :&on-cancel);

=end code

=head2 Store events

Pass a C<store> (anything answering C<dispatch(Str $event, *%payload)>
— see C<JobQueue::EventSink>) and the queue narrates the lifecycle:

  queue/job-enqueued  queue/job-started  queue/job-phase-changed
  queue/job-progressed  queue/job-cancelling
  queue/job-finished  queue/job-failed  queue/job-cancelled
  queue/job-superseded

Every event carries C<queue> (the queue name) and C<job> (the
snapshot Hash from C<job-snapshot>). Leaving C<store> undefined
disables the stream entirely.

Guarantees worth relying on:

=item B<Exactly one terminal event per job.> For a job that ever
  started it comes from the runner's then-hook at true drain end; for
  a job cancelled while pending it comes from inside C<cancel>.
=item B<Terminal routing keys on the kept completion value>, never on
  C<$job.state>: C<done> → C<job-finished>, C<superseded> →
  C<job-superseded>, C<cancelled> → C<job-cancelled>, anything else →
  C<job-failed>. A runner that self-finishes C<'cancelled'> is a user
  cancel, not a red failure.
=item C<queue/job-cancelling> B<precedes> C<&on-cancel>, because
  production on-cancel callbacks resolve the runner promise inline and
  the deferred terminal must not beat the announcement into the store.

=head2 Counting

C<total-count> is the physical view (running + pending, drainers
included). C<live-count> is the logical one — pending plus running
jobs whose completion is not Kept. Lock a reply box on C<live-count>;
size a backend pool on C<total-count>.

=end pod

use JobQueue::Job;
use JobQueue::Observability;

unit class JobQueue::Queue;

has Str  $.name is required;
has      &.is-parallel-now is required;
has      &.run-job is required;

#|( Optional event sink. When given, the queue dispatches the
    C<queue/job-*> lifecycle across it as
    C<< .dispatch($event, queue => $.name, job => %snapshot) >>.
    Deliberately untyped so any object answering C<dispatch> works
    without having to compose C<JobQueue::EventSink>; leaving it
    undefined disables the event stream entirely — used by tests that
    don't want to wire a store, and as a safety net so a store-less
    queue never crashes on dispatch. )
has $.store;

#|( Structured log sink. Defaults to the silent C<NullLog>; see
    C<JobQueue::Observability> for the three-verb contract. )
has $.log = NullLog.new;

#|( Span tracer. Defaults to the silent C<NullTracer>; see
    C<JobQueue::Observability>. Every span start is gated behind
    C<.enabled>, so the null path allocates nothing. )
has $.tracer = NullTracer.new;

#|( Cap on concurrently-running jobs while the queue is in PARALLEL
    mode (0 = unbounded, the default). Serial mode is always exactly
    one. Counts every running job, including cancelled/superseded
    ones still draining — they occupy the physical backend until
    their work returns. )
has Int $.max-concurrent = 0;

has @!pending;
has %!running{Str};

# Single lock guards both @!pending and %!running. All queue mutations
# (enqueue, dispatch, completion-removal, cancel) take it. The lock
# is held only across O(1) data-structure work; the actual job runs
# in a `start { ... }` Promise outside the lock so the queue doesn't
# serialise on the runner.
has Lock $!lock = Lock.new;

#|( Snapshot keys the queue owns. C<Job.snapshot-extras> can never
    write these, present or absent — a subclass must not be able to
    make its ledger row lie about its own state. )
my constant RESERVED-SNAPSHOT-KEYS = Set.new(<
    id job-id kind scope-id state enqueued-at started-at finished-at
    error phase cancellable lane dedup-key priority queue-name
>);

#|( Add a job to the queue. If we're in parallel mode (or the
    running slot is free in serial mode), the job starts immediately;
    otherwise it sits pending until a slot frees up.

    Returns the job for chaining (e.g. C<my $j = $q.enqueue(...);
    await $j.completion>) — or, on a dedup hit, the existing live job
    carrying the same key. )
method enqueue(Job:D $job --> Job:D) {
    my $span = $.tracer.enabled
        ?? $.tracer.start('queue.enqueue', cat => 'queue',
            args => self!trace-job-args($job))
        !! Nil;
    # Dedup contract: a LIVE job (completion not Kept) carrying the
    # same dedup-key wins — the caller gets the existing job back and
    # nothing is enqueued. Cancel/supersede keep the completion
    # Promise synchronously, so a cancelled-but-still-draining job
    # never absorbs a fresh enqueue (the rapid A→B→A case).
    my $existing;
    # Lookup and insertion are one state transition. Splitting them across
    # two lock acquisitions lets concurrent callers both observe a miss and
    # enqueue the same key, which defeats the whole dedup contract precisely
    # when a queue has several producers.
    $!lock.protect: {
        if $job.dedup-key.chars {
            $existing = %!running.values.first({
                .dedup-key eq $job.dedup-key && .completion.status !~~ Kept
            }) // @!pending.first({
                .dedup-key eq $job.dedup-key && .completion.status !~~ Kept
            });
        }
        @!pending.push: $job unless $existing.defined;
    }
    with $existing {
        $.log.debug("$!name/job-deduped",
            :key($job.dedup-key), :existing-id($_.id),
            :dropped-id($job.id));
        $span.finish(deduped => True) with $span;
        return $_;
    }
    self!dispatch-event('queue/job-enqueued', $job);
    self.tick;
    with $span {
        .finish(pending => self.pending-count, running => self.running-count);
    }
    $job;
}

#|( Find the live (pending or running, completion not Kept) job
    carrying C<$key>, or Nil. )
method find-by-dedup-key(Str:D $key) {
    $!lock.protect: {
        %!running.values.first({
            .dedup-key eq $key && .completion.status !~~ Kept
        }) // @!pending.first({
            .dedup-key eq $key && .completion.status !~~ Kept
        });
    }
}

#|( Build a serialisable snapshot of the job for store events. Keeps
    only plain scalars — the full Job object isn't safe to drop into
    a store (it carries closures, Promises and a Lock).

    The queue-owned fields come first; C<< $job.snapshot-extras >>
    then contributes whatever the composing class wants its UI to
    see, minus any attempt to write a reserved key. )
method job-snapshot(Job:D $job --> Hash) {
    my %s = %(
        id          => $job.id,
        job-id      => $job.id,
        kind        => ($job.?kind // ''),
        scope-id    => $job.scope-id,
        state       => $job.state,
        enqueued-at => $job.enqueued-at.Num,
        priority    => $job.priority,
        cancellable => $job.cancellable,
    );
    %s<started-at>  = $job.started-at.Num  if $job.started-at.defined;
    %s<finished-at> = $job.finished-at.Num if $job.finished-at.defined;
    %s<error>       = $job.error      if ($job.error      // '').chars;
    %s<phase>       = $job.phase      if ($job.phase      // '').chars;
    %s<lane>        = $job.lane       if ($job.lane       // '').chars;
    %s<dedup-key>   = $job.dedup-key  if ($job.dedup-key  // '').chars;
    %s<queue-name>  = $job.queue-name if ($job.queue-name // '').chars;
    my %extras = $job.snapshot-extras;
    for %extras.kv -> $k, $v {
        %s{$k} = $v unless RESERVED-SNAPSHOT-KEYS{$k};
    }
    %s;
}

method !dispatch-event(Str:D $event, Job:D $job, :%extra) {
    return without $.store;
    my %snap = self.job-snapshot($job);
    %snap{.key} = .value for %extra;
    $.store.dispatch($event,
        queue => $.name,
        job   => %snap,
    );
}

#|( Record a phase transition on a RUNNING job and broadcast it as
    C<queue/job-phase-changed>. Guarded: terminal, cancelled, or
    not-currently-running jobs are ignored so a stale late note can
    never resurrect a finished job's ledger row. Producers own any
    rate limiting. )
method note-phase(Job:D $job, Str:D $phase --> Nil) {
    return if $job.completion.status ~~ Kept || $job.cancelled;
    return unless $!lock.protect({ %!running{$job.id}:exists });
    $job.phase = $phase;
    self!dispatch-event('queue/job-phase-changed', $job);
}

#|( Broadcast incremental progress for a RUNNING job as
    C<queue/job-progressed> (snapshot + C<< progress => {value, max,
    node} >>). Same guards as C<note-phase>; producers throttle. )
method note-progress(
    Job:D $job,
    :$value!, :$max!, Str :$node = ''
    --> Nil
) {
    return if $job.completion.status ~~ Kept || $job.cancelled;
    return unless $!lock.protect({ %!running{$job.id}:exists });
    self!dispatch-event('queue/job-progressed', $job,
        extra => %( progress => %( :$value, :$max, :$node ) ));
}

method !trace-job-args(Job:D $job --> Hash) {
    %(
        queue    => $.name,
        job_id   => $job.id,
        kind     => ($job.?kind // ''),
        scope_id => $job.scope-id,
        state    => $job.state,
    );
}

#|( Try to start as many pending jobs as the parallelism policy,
    C<max-concurrent> cap, and lanes allow. Called from C<enqueue>
    and per-frame from the host's loop. Cheap when nothing's pending
    (O(1) peek, and crucially no C<is-parallel-now> evaluation — that
    closure may do a DB read and must run neither on idle frames nor
    under the queue lock).

    Lane semantics: at most one LIVE job per non-empty lane runs at
    a time; a pending job whose lane is busy is skipped (jobs in
    other lanes may overtake), preserving FIFO within each lane.
    "Live" excludes running jobs whose completion is already Kept —
    a cancelled/superseded job still draining its unabortable work
    releases its lane immediately so the successor can queue up.
    The serial gate and C<max-concurrent> cap, by contrast, count
    ALL running jobs: the drain still physically occupies the
    backend.

    Priority semantics: each freed slot goes to the highest
    C<Job.priority> among lane-eligible pending jobs, FIFO within a
    level, so a short pipeline-critical job never sits a full bulk
    job behind work that could just as well run later. )
method tick(--> Nil) {
    return unless $!lock.protect({ @!pending.elems > 0 });

    my $span = $.tracer.enabled
        ?? $.tracer.start('queue.tick', cat => 'queue',
            args => %(queue => $.name))
        !! Nil;
    # Evaluated OUTSIDE the lock: production closures read config
    # from a database.
    my Bool $parallel = ?&!is-parallel-now();
    my Int $started = 0;
    loop {
        my $job;
        $!lock.protect: {
            my Bool $capacity = @!pending.elems > 0
                && ($parallel ?? ($!max-concurrent == 0
                                  || %!running.elems < $!max-concurrent)
                              !! %!running.elems == 0);
            if $capacity {
                my %busy-lanes;
                for %!running.values -> $r {
                    next if $r.completion.status ~~ Kept;
                    %busy-lanes{$r.lane} = True if $r.lane.chars;
                }
                # Highest-priority lane-eligible job wins the slot;
                # FIFO within a level. Lane-blocked jobs are skipped,
                # not blocking — a zero-priority job in a free lane
                # may overtake a lane-blocked prioritised one.
                my $idx;
                my Int $best-priority;
                for @!pending.kv -> $i, $p {
                    next if $p.lane.chars && %busy-lanes{$p.lane};
                    if !$idx.defined || $p.priority > $best-priority {
                        $idx = $i;
                        $best-priority = $p.priority;
                    }
                }
                with $idx {
                    $job = @!pending.splice($idx, 1)[0];
                    %!running{$job.id} = $job;
                }
            }
        }
        last without $job;
        $started++;
        self!start($job);
    }
    with $span {
        .finish(started => $started, pending => self.pending-count, running => self.running-count);
    }
}

#|( Spawn the runner for C<$job>. C<&!run-job> returns a Promise; we
    chain a C<then> that removes the job from the running set and
    ticks the queue so the next pending slot fills. Exceptions inside
    the runner are caught and surfaced via the job's completion
    promise being kept with C<'error'> — never lets a bad runner take
    down the queue. )
method !start(Job:D $job) {
    # begin-running is atomic with the terminal transitions: a cancel
    # that wins in the window between tick() publishing the job to
    # %!running and this spawn already announced 'cancelling' and owns
    # the terminal — no state clobber, no phantom job-started. The
    # runner still runs either way; its first cancelled-guard exits
    # immediately and the then-hook below releases the slot through
    # the one unwind path.
    if $job.begin-running {
        $.tracer.instant('queue.job-started', cat => 'queue',
            args => self!trace-job-args($job));
        $.log.info("$!name/job-started",
            :job-id($job.id), :scope-id($job.scope-id));
        self!dispatch-event('queue/job-started', $job);
    }
    my $run-span = $.tracer.enabled
        ?? $.tracer.start('queue.job-run', cat => 'queue',
            args => self!trace-job-args($job), slow-ms => 25e0)
        !! Nil;

    my $promise = &!run-job($job);
    # Defensive: a runner that returns Nil or a non-Promise would
    # otherwise stall the queue. Coerce to a Promise we can chain.
    unless $promise ~~ Promise {
        $promise = Promise.kept('done');
    }

    $promise.then: -> $p {
        my $outcome = $p.status ~~ Kept
            ?? ($p.result // 'done').Str
            !! 'error';
        # A broken runner promise is a failure the runner never got to
        # annotate — capture the exception message so the ledger row
        # has a cause.
        if $p.status !~~ Kept && !$job.error.chars {
            $job.error = ($p.cause.?message // 'runner failed').Str;
        }
        # Atomic terminal transition, and the SOLE terminal emission
        # point for every job that started: the hook winning, a runner
        # self-finish, and the cancel/supersede paths — which announce
        # 'cancelling' at cancel time and get their terminal here,
        # once the runner has actually unwound. Emitting a running
        # job's terminal at cancel time made the ledger drop the row
        # while the drain still occupied the backend: the queue looked
        # empty yet blocked, which is exactly the lie this defers.
        # Routing keys on completion.result: the kept value is
        # immutable, while $job.state loses races by design.
        $job.finish($outcome);
        my Str $final = $job.completion.result.Str;
        my Str $terminal-event = do given $final {
            when 'done'       { 'queue/job-finished' }
            # Grey system terminal (apply guard tripped mid-run, or a
            # deferred supersede-cancel) — not a red failure.
            when 'superseded' { 'queue/job-superseded' }
            # User cancel, whether the cancel path won or the runner
            # observed the token and self-finished first — never a
            # red job-failed.
            when 'cancelled'  { 'queue/job-cancelled' }
            default           { 'queue/job-failed' }
        };
        # extra<state> pins the snapshot to the immutable outcome for
        # the same reason the routing does.
        self!dispatch-event($terminal-event, $job,
            extra => %( state => $final ));
        $.log.info("$!name/job-finished",
            :job-id($job.id), :outcome($final), :won-by($job.finished-by));
        $!lock.protect: {
            %!running{$job.id}:delete;
        }
        with $run-span {
            .finish(outcome => $final);
        }
        self.tick;
    };
}

#|( Cancel a job by id. If pending, drops it from the queue and emits
    the terminal event immediately (nothing ever ran). If running,
    keeps the cancel-token and finishes the job synchronously so the
    eventual runner-return can't overwrite the terminal state — but
    the TERMINAL EVENT is deferred to the runner's then-hook: until
    the runner unwinds, the job still physically occupies the
    backend, and the ledger dropping the row early is how the queue
    used to look empty while blocking. What fires here instead is
    C<queue/job-cancelling> (non-terminal; snapshot pinned to
    state/phase C<cancelling>, C<cancellable> False), dispatched
    BEFORE C<&on-cancel> — production on-cancel callbacks resolve the
    runner promise inline, and the deferred terminal must not beat
    the cancelling announcement into the store.

    C<&on-cancel($job)> lets the caller fire backend-side aborts
    (safe no-op when nothing is in flight yet — pending jobs get it
    too, since caller cleanup is the same whether the job ever ran).

    C<:$outcome> defaults to C<'cancelled'> (user intent); pass
    C<'superseded'> for system-initiated replacement (input changed,
    dependency failed) — the eventual terminal is
    C<queue/job-superseded> instead, which a ledger records as a
    grey, non-retryable terminal.

    Returns True iff a matching job was found AND this call performed
    the terminal transition (a lost race against the runner's own
    completion returns False and emits nothing — the job genuinely
    finished first, and the then-hook owns its terminal). )
method cancel(Str:D $job-id, :&on-cancel, Str :$outcome = 'cancelled' --> Bool) {
    my $span = $.tracer.enabled
        ?? $.tracer.start('queue.cancel', cat => 'queue',
            args => %(queue => $.name, job_id => $job-id, outcome => $outcome))
        !! Nil;
    my $event = $outcome eq 'superseded'
        ?? 'queue/job-superseded'
        !! 'queue/job-cancelled';
    my $running-job;
    my $pending-job;
    $!lock.protect: {
        $running-job = %!running{$job-id};
        unless $running-job.defined {
            my $idx = @!pending.first({ .id eq $job-id }, :k);
            with $idx {
                $pending-job = @!pending[$idx];
                @!pending.splice($idx, 1);
            }
        }
    }
    for ($pending-job, $running-job).kv -> $i, $job {
        next without $job;
        $job.request-cancel;
        my Bool $won = $job.finish($outcome, :by<cancel>);
        if $won {
            $.log.info(
                "$!name/job-{$outcome}-{$i == 0 ?? 'pending' !! 'running'}",
                :job-id($job-id), :scope-id($job.scope-id));
            if $i == 1 {
                # RUNNING: announce the drain, then let on-cancel fire
                # backend aborts. Terminal comes from the then-hook.
                self!dispatch-event('queue/job-cancelling', $job,
                    extra => %(
                        state       => 'cancelling',
                        phase       => 'cancelling',
                        cancellable => False,
                    ));
                &on-cancel($job) if &on-cancel;
            } else {
                # PENDING: no runner, no drain — terminal now.
                &on-cancel($job) if &on-cancel;
                self!dispatch-event($event, $job);
            }
        }
        with $span {
            .finish(found => True, won => $won,
                state => ($i == 0 ?? 'pending' !! 'running'));
        }
        return $won;
    }
    with $span {
        .finish(found => False);
    }
    False;
}

#|( Cancel the most recent job (running first, then most-recently-
    enqueued pending) belonging to C<$scope>. The intent this serves
    is "abort whatever I just started here", which means the most
    recent thing burning cycles. Scope comparison is C<eqv>, so Int
    and Str scope ids both work. Returns True if a job was found and
    cancelled. )
method cancel-most-recent-in-scope($scope, :&on-cancel --> Bool) {
    my $target-id;
    $!lock.protect: {
        # Prefer running jobs — those are actively consuming the
        # backend. Among them, pick the one started latest.
        my @running = %!running.values
            .grep({ .scope-id eqv $scope })
            .sort({ ($^a.started-at // Instant.from-posix(0))
                cmp ($^b.started-at // Instant.from-posix(0)) });
        if @running.elems {
            $target-id = @running.tail.id;
        } else {
            # No running job — most-recently-enqueued pending.
            my @pend = @!pending.grep({ .scope-id eqv $scope });
            $target-id = @pend.tail.id if @pend.elems;
        }
    }
    return False without $target-id;
    self.cancel($target-id, :&on-cancel);
}

#|( Snapshot of currently-running + pending jobs for C<$scope>, in
    dispatch order (running first, then pending head-first).
    Read-only — callers must not mutate. Useful for status badges. )
method jobs-for-scope($scope --> Seq) {
    $!lock.protect: {
        my @out = %!running.values.grep({ .scope-id eqv $scope });
        @out.append: @!pending.grep({ .scope-id eqv $scope });
        @out.Seq;
    }
}

#| Number of jobs currently in flight (running, not pending).
method running-count(--> Int) {
    $!lock.protect: { %!running.elems }
}

#| Number of jobs waiting to start.
method pending-count(--> Int) {
    $!lock.protect: { @!pending.elems }
}

#| All jobs combined (running + pending). For diagnostics + tests.
method total-count(--> Int) {
    $!lock.protect: { %!running.elems + @!pending.elems }
}

#|( Jobs that still represent LIVE work: pending + running whose
    completion is not Kept. Excludes cancelled/superseded runners
    still draining unabortable work — those occupy the backend (the
    serial gate and C<max-concurrent> still count them) but nothing
    the user is waiting on depends on them; a new enqueue simply
    pends behind the drain. Reply-lock style predicates key on this;
    C<total-count> keeps the physical view. )
method live-count(--> Int) {
    $!lock.protect: {
        @!pending.elems
        + %!running.values.grep({ .completion.status !~~ Kept }).elems;
    }
}

#| True if no jobs are running or pending.
method is-empty(--> Bool) {
    self.total-count == 0;
}

#|( Look up a job by id. Returns the C<Job> or C<Nil>. Used by
    "is this thing still active on the queue?" lookups. )
method find-job(Str:D $job-id --> Job) {
    $!lock.protect: {
        with %!running{$job-id} { return $_ };
        with @!pending.first({ .id eq $job-id }) { return $_ };
        Nil;
    }
}

#|( Snapshot of currently-running jobs (no order guarantee). Returned
    as an Array so callers can iterate without holding the lock. )
method running-jobs(--> Array) {
    $!lock.protect: { %!running.values.Array }
}

#|( Snapshot of pending jobs, head-first (next-to-run leads). Array
    so callers can iterate without holding the lock. )
method pending-jobs(--> Array) {
    $!lock.protect: { @!pending.clone }
}
