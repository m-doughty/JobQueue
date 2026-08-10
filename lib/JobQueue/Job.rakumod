=begin pod

=head1 NAME

JobQueue::Job - the unit of work a JobQueue schedules

=head1 DESCRIPTION

A C<JobQueue::Queue> operates on B<Job> objects rather than
dispatching function calls directly. That indirection is the whole
point: a Job carries

=item the immutable parameters captured at enqueue time (so a job's
  context doesn't drift while it sits pending);
=item runtime state (C<state>, C<started-at>, C<finished-at>);
=item a per-job cancel C<Promise>, kept by C<JobQueue::Queue.cancel>;
=item a per-job completion C<Promise>, kept once the job reaches a
  terminal state;
=item scheduling policy — C<lane>, C<priority>, C<dedup-key>;
=item its place in the dependency DAG — C<depends-on>, C<after>,
  C<result>, C<dep-results>.

C<Job> is a B<role>. You compose it into whatever class models your
actual work, and add your own fields:

=begin code :lang<raku>

use JobQueue::Job;

class RenderJob does Job {
    has Str $.template is required;
    has %.params;
    has Int $.output-id is rw;

    method snapshot-extras(--> Hash) {
        my %e = template => $!template;
        %e<output-id> = $!output-id if $!output-id.defined;
        %e;
    }
}

my $job = RenderJob.new(
    scope-id => 'doc-42',
    template => 'invoice',
    lane     => 'render:doc-42',
);

=end code

=head2 States

States follow a linear progression:

  pending → running → done | error | cancelled | superseded

The terminal states are mutually exclusive, and C<completion> is Kept
with the terminal state string once the job gets there. Runners may
invent more specific failure strings (a streaming runner might finish
with C<broken>); the queue treats B<any> terminal state other than
C<done>, C<cancelled> and C<superseded> as a failure.

When a job fails, the runner should record a human-readable reason on
C<error> before calling C<finish>. The queue's snapshot carries it
into the store event, where a retry UI can render it.

C<cancelled> means "a human asked for this to stop", C<superseded>
means "the system replaced or abandoned this". Retry UIs typically
render the former as red-and-retryable and the latter as grey.

=head2 Atomicity

C<finish>, C<request-cancel> and C<begin-running> all take a single
per-job lock, and each returns True B<iff this call> performed the
transition. That boolean is the ownership token: whoever won the
transition owns the terminal-event emission, and everyone else backs
off silently. Without it you get double-emission and
last-writer-wins state clobbering — a cancel landing in the window
between the queue publishing a job to its running set and the runner
spawning would otherwise be stomped back to C<running>.

=begin code :lang<raku>

# In a runner:
if $job.cancelled {
    return;                 # the cancel path owns the terminal
}
...
$job.error = 'backend refused the request';
$job.finish('error');       # True — we own it

=end code

=head2 Scope

C<scope-id> is required and B<untyped>: it is whatever identifies the
logical owner of the work — a conversation id (Int), a document slug
(Str), a tenant uuid. The queue and coordinator only ever compare it
with C<eqv>, never with C<==> or C<eq>, so mixed-type scopes are safe.

=head2 Extension points

Two methods let a composing class add payload without the queue ever
having to know the class exists:

=item C<snapshot-extras> — merged into the store-event snapshot. Fixed
  queue-owned keys always win, so you can't accidentally rewrite
  C<state> or C<id>.
=item C<dep-extras> — merged into B<dependents'> C<dep-results> entry
  for this job when the coordinator releases them. C<state> and
  C<result> always win.

Override with the B<exact> signature — C<method snapshot-extras(-->
Hash)>. Raku will happily accept a different one and silently leave
the role's version in charge for the call sites that don't match.

=begin code :lang<raku>

class PromptJob does Job {
    has Int $.message-id is rw;
    method snapshot-extras(--> Hash) {
        $!message-id.defined ?? %( message-id => $!message-id ) !! %();
    }
    method dep-extras(--> Hash) {
        $!message-id.defined ?? %( message-id => $!message-id ) !! %();
    }
}

=end code

=head2 Cancellation

C<cancel-token> is a fresh Promise per job. The queue's C<cancel>
keeps it; the runner checks C<$job.cancelled> at every guard point
that would mutate shared state. This is what stops a slow upstream
(one final token arriving after the user hit cancel) from writing a
"success" after the cancel handler already wrote "cancelled".

Cancellation is cooperative by construction: the queue keeps the
token and calls your C<on-cancel>; B<the runner> is responsible for
actually unwinding.

=head1 EXPORTS

The C<Job> role and the C<keep-once> sub. Both are also package-scoped
(C<our>), so a downstream module that wraps this role can re-export
them by name without importing — which is what lets a host application
keep its own C<Model::Job>-style facade in front of this one:

=begin code :lang<raku>

need JobQueue::Job;                 # load, don't import

unit module MyApp::Job;

constant &keep-once is export = &JobQueue::Job::keep-once;

role Job does JobQueue::Job::Job is export {
    method owner-id { self.scope-id }    # domain vocabulary
}

=end code

C<need> rather than C<use> matters here: importing C<Job> and then
declaring a role of the same name in the same compunit makes the
wrapper module export two symbols called C<Job>, and its consumers
die at C<use> time.

=end pod

use UUID::V4;

unit module JobQueue::Job;

#|( Atomically keep a bare Promise, tolerating a lost race. Promise
    has no test-and-set: `keep unless Kept` is a check-then-act race,
    and the loser THROWS "Promise already kept" — from a worker
    that's an unobserved detonation, from a frame tick it can take
    the app down. `try` around the keep IS the atomic idiom: the
    runtime's own vow check is the arbiter, and a lost race is a
    benign outcome for the done/end promises this guards (both
    racers wanted the promise settled; the winner's value stands —
    matching Job.finish's winner-owns-the-transition contract).
    Returns True iff this call won. No-op on undefined promises.

    =begin code :lang<raku>
    my $p = Promise.new;
    say keep-once($p, 'first');    # True
    say keep-once($p, 'second');   # False — and no throw
    say $p.result;                 # first
    =end code )
our sub keep-once($promise, $value --> Bool) is export {
    return False without $promise;
    so (try { $promise.keep($value); True });
}

role Job is export {
    has Str     $.id            = ~uuid-v4;

    #|( Logical owner of the work — conversation id, document slug,
        tenant uuid, whatever your domain uses. Deliberately untyped:
        the queue and coordinator only ever compare scopes with
        C<eqv>, so Int and Str scopes both work (and can coexist). )
    has         $.scope-id is required;

    has Str     $.state is rw   = 'pending';
    has Promise $.cancel-token  = Promise.new;
    has Promise $.completion    = Promise.new;
    has Instant $.enqueued-at   = now;
    has Instant $.started-at  is rw;
    has Instant $.finished-at is rw;

    #|( Human-readable failure reason, set by the runner before it
        finishes the job with a failure state. Empty on success and
        cancellation. Carried into queue store events by
        C<JobQueue::Queue.job-snapshot> so failed jobs surface their
        cause in whatever UI renders the ledger. )
    has Str     $.error is rw   = '';

    #|( Live-job identity. When non-empty,
        C<JobQueue::Queue.enqueue> returns an existing pending/running
        job carrying the same key instead of enqueueing a duplicate
        (see the queue's dedup contract). Empty string disables
        dedup. )
    has Str     $.dedup-key     = '';

    #|( Serialization lane. Jobs sharing a non-empty lane run at most
        one at a time (FIFO within the lane) even when the queue is
        in parallel mode; jobs in other lanes may overtake. Empty
        string = no lane constraint. )
    has Str     $.lane          = '';

    #|( Dispatch priority. When a queue slot frees, the highest
        priority among lane-eligible PENDING jobs starts first; FIFO
        within a level. Reserve non-zero values for short
        pipeline-critical jobs that must never sit behind long bulk
        work — keep bulk at 0, so starvation requires a continuous
        stream of prioritised jobs. )
    has Int     $.priority      = 0;

    #|( Hard dependencies (job ids). C<JobQueue::Coordinator> holds
        this job until every hard dep completes 'done'; a hard dep
        failing / being cancelled / superseded supersedes this job
        transitively. Opaque to the queue itself. )
    has Str     @.depends-on;

    #|( Soft dependencies (job ids): wait until the dep reaches ANY
        terminal state, then run regardless of its outcome. Used for
        best-effort precursors whose failure must not kill the
        dependent. )
    has Str     @.after;

    #| Name of the queue the coordinator submitted this job to.
    has Str     $.queue-name is rw = '';

    #|( Current phase string ('' = none), maintained via
        C<JobQueue::Queue.note-phase> and carried into job snapshots
        so a ledger can show pipeline stages. )
    has Str     $.phase is rw   = '';

    #|( Whether a UI may offer to cancel this job. Jobs whose work has
        no abort surface can still be cancelled logically (the result
        is discarded at apply time), so this defaults True; set False
        only when cancellation would be a pure lie. )
    has Bool    $.cancellable   = True;

    #|( Runner-written outcome payload. Injected into dependents'
        C<dep-results> by the coordinator when this job completes. )
    has %.result is rw;

    #|( dep-id => { state, result, ...dep-extras } for every declared
        dependency, injected by the coordinator at release time — the
        DAG's data handoff. )
    has %.dep-results is rw;

    # One lock guards both terminal transitions. finish/request-cancel
    # race from the queue then-hook, cancel paths, and runners; the
    # boolean return tells the caller whether IT performed the
    # transition and therefore owns the terminal event emission.
    has Lock $!terminal-lock = Lock.new;

    #|( True iff C<cancel-token> has been kept. Cheap, repeatable
        check used by runners at every guard point. )
    method cancelled(--> Bool) {
        $!cancel-token.status ~~ Kept;
    }

    #|( Which side performed the terminal transition: 'runner' (the
        default — runner self-finish or the queue then-hook) or
        'cancel' (a queue cancel/supersede path). Diagnostic: for any
        job that started, the queue's then-hook is the sole
        terminal-event emitter (routing on the kept completion
        value), so a cancel racing a runner self-finish can neither
        mislabel nor double-emit. Written atomically inside the
        winning C<finish> transition. )
    has Str $.finished-by is rw = '';

    #|( Drive the job to a terminal state. ATOMIC and idempotent:
        returns True iff THIS call performed the transition. The kept
        C<completion> value (this call's C<$final-state> when it
        wins) is what the queue's then-hook routes the single
        terminal event on — C<$.state> is best-effort display state,
        not the emission key. )
    method finish(Str:D $final-state, Str :$by = 'runner' --> Bool) {
        $!terminal-lock.protect: {
            return False if $!completion.status ~~ Kept;
            $!state = $final-state;
            $!finished-at = now;
            $!finished-by = $by;
            $!completion.keep($final-state);
            True;
        }
    }

    #|( Atomically keep the cancel token. True iff this call newly
        requested cancellation. )
    method request-cancel(--> Bool) {
        $!terminal-lock.protect: {
            return False if $!cancel-token.status ~~ Kept;
            $!cancel-token.keep('cancelled');
            True;
        }
    }

    #|( Mark the job running (state + started-at), atomically with
        the terminal transitions: a cancel that wins in the window
        between the queue publishing the job to its running set and
        the runner spawn must never be clobbered back to 'running'
        (nor produce a phantom job-started event). True iff the
        transition happened. )
    method begin-running(--> Bool) {
        $!terminal-lock.protect: {
            return False if $!completion.status ~~ Kept;
            $!state = 'running';
            $!started-at = now;
            True;
        }
    }

    #|( Subclass payload merged into the store-event snapshot built by
        C<JobQueue::Queue.job-snapshot>. Return whatever fields your
        UI needs to render this job — ids, targets, previews. The
        queue's own fixed keys always win, so extras can never rewrite
        C<id>, C<state>, C<error> and friends.

        Override with this EXACT signature. Default: no extras. )
    method snapshot-extras(--> Hash) { %() }

    #|( Subclass payload merged into C<dep-results{ this-job-id }> in
        every dependent, injected by C<JobQueue::Coordinator> at
        release time. Use it to hand a dependent the ids or handles
        its predecessor produced. C<state> and C<result> always win.

        Override with this EXACT signature. Default: no extras. )
    method dep-extras(--> Hash) { %() }
}
