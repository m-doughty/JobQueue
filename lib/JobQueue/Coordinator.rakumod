=begin pod

=head1 NAME

JobQueue::Coordinator - named-queue registry + cross-queue job dependency DAG

=head1 SYNOPSIS

=begin code :lang<raku>

use JobQueue::Job;
use JobQueue::Queue;
use JobQueue::Coordinator;

my $coord = JobQueue::Coordinator.new(:$store);
$coord.register-queue('text-backend',  $text-queue);
$coord.register-queue('image-backend', $image-queue);

# Declare the whole turn as a flat DAG — no nesting, no .then pyramids:
my $state = StateJob.new(scope-id => $conv);
$coord.submit('text-backend', $state);
$coord.submit('text-backend',
    ReconcileJob.new(scope-id => $conv, depends-on => [$state.id]));
$coord.submit('image-backend',
    ImageJob.new(scope-id => $conv, depends-on => [$state.id]));

# Per frame:
$coord.tick;

=end code

=head1 DESCRIPTION

The coordinator owns every named C<JobQueue::Queue> and the
cross-queue dependency graph. A submitted job whose dependencies
aren't satisfied is B<held> here (announced as C<queue/job-waiting>);
the per-frame C<tick> releases it onto its queue once:

=item every B<hard> dependency (C<depends-on>) completed C<done>, and
=item every B<soft> dependency (C<after>) reached ANY terminal state.

Dependency resolution is tick-driven, not C<.then>-hooked. Releases
happen on the caller's thread in a deterministic order, which is the
whole point: the job chain becomes a declaration you can read rather
than a callback topology you have to simulate in your head.

=head2 Data handoff

At release, each dependency's outcome is injected into the dependent's
C<dep-results>:

=begin code :lang<raku>

# In the dependency's runner:
$job.result = %( summary => $text, tokens => $n );
$job.finish('done');

# In the dependent's runner:
my %dep = $job.dep-results{$dep-id};
say %dep<state>;              # 'done'
say %dep<result><summary>;    # the payload
say %dep<message-id>;         # whatever the dep's dep-extras added

=end code

C<state> and C<result> are always present and always win; anything
else comes from the dependency's C<dep-extras>. The handoff is
in-memory by design — routing it through a shared store slot would
race concurrent submissions and leak stale results between them.

=head2 Failure propagation

A hard dependency terminating in anything but C<done> B<supersedes>
the held dependent, and the supersede cascades transitively over hard
edges. Soft dependents run regardless — that's what "soft" buys you: a
best-effort precursor whose failure must not kill the work behind it.

Superseding a queued or running job routes through the owning queue's
C<cancel(:outcome<superseded>)>, so its own on-cancel/backend-abort
plumbing fires and the ledger records a grey, non-retryable terminal.

Unknown hard dependency ids (never submitted here) supersede the
dependent immediately with a logged error — fail-fast beats a job
silently held forever. Unknown B<soft> deps are skipped: a
best-effort edge to something that never existed is satisfied by
definition.

=head2 Held-job watchdog

C<held-warn-after> (default 300 seconds) bounds silence, not the hold
itself: a job held longer than that logs C<coordinator/job-held-too-long>
once, with its dependency ids, so a dependency that never terminates
shows up in the log instead of as a mysteriously idle pipeline.

=head2 Counting

C<total-active> is the number of things the user is actually waiting
on: held jobs plus every queue's C<live-count>. It deliberately
excludes cancelled jobs still draining an unabortable call — those
occupy the backend but nothing waits on them, and holding an input
locked for their sake is a lie.

=end pod

use JobQueue::Job;
use JobQueue::Observability;

unit class JobQueue::Coordinator;

#|( Optional event sink for C<queue/job-waiting> and held-supersede
    dispatches. Same duck-typed contract as the queue's — see
    C<JobQueue::EventSink>. )
has $.store;

#|( Structured log sink. Defaults to the silent C<NullLog>; see
    C<JobQueue::Observability>. )
has $.log = NullLog.new;

#|( Warn when a job has been held longer than this many seconds —
    a dependency that never terminates would otherwise hold it
    silently forever. )
has Num $.held-warn-after = 300e0;

has %!queues;               # name => queue
has %!jobs;                 # job-id => { job, queue-name, held(Bool), held-at }
has %!dependents;           # dep-id => [dependent job-ids] (hard + soft)
has %!warned-held;          # job-id => True once the held warning fired
has Lock $!lock = Lock.new;

#|( Register a queue under C<$name>. Dies on a duplicate name — a
    silently replaced queue would strand every job already routed to
    the old one. )
method register-queue(Str:D $name, $queue --> Nil) {
    $!lock.protect: {
        die "JobQueue::Coordinator: queue '$name' already registered"
            if %!queues{$name}:exists;
        %!queues{$name} = $queue;
    }
}

#| The queue registered under C<$name>, or Nil.
method queue(Str:D $name) {
    $!lock.protect: { %!queues{$name} }
}

#|( Submit a job to the named queue, honoring dedup and dependencies.
    Returns the job that will actually run: the caller's job, or an
    existing live job with the same dedup-key. Dies when the queue
    name is unknown — a typo'd queue name is a programming error, not
    a runtime condition. )
method submit(Str:D $queue-name, Job:D $job) {
    my $queue = self.queue($queue-name)
        // die "JobQueue::Coordinator: no queue named '$queue-name'";
    $job.queue-name = $queue-name;

    # Dedup across held jobs too — a held duplicate is just as live.
    if $job.dedup-key.chars {
        my $held-dup = $!lock.protect: {
            %!jobs.values.first({
                $_<held>
                && $_<job>.dedup-key eq $job.dedup-key
                && $_<job>.completion.status !~~ Kept
            });
        };
        with $held-dup {
            $.log.debug('coordinator/job-deduped-held',
                :key($job.dedup-key), :existing-id($_<job>.id));
            return $_<job>;
        }
        with $queue.find-by-dedup-key($job.dedup-key) {
            $.log.debug('coordinator/job-deduped-queued',
                :key($job.dedup-key), :existing-id(.id));
            return $_;
        }
    }

    $!lock.protect: {
        %!jobs{$job.id} = %( :$job, :queue-name($queue-name), :held(False),
            held-at => Instant );
        for flat $job.depends-on, $job.after -> $dep-id {
            %!dependents{$dep-id} //= [];
            %!dependents{$dep-id}.push: $job.id;
        }
    }

    given self!dep-status($job) {
        when 'ready' {
            self!release($job, $queue);
        }
        when 'blocked' {
            $!lock.protect: {
                %!jobs{$job.id}<held> = True;
                %!jobs{$job.id}<held-at> = now;
            }
            self!dispatch-waiting($job, $queue);
        }
        default {   # 'doomed' — a hard dep already failed/cancelled
            self!supersede-held($job, reason => 'dep-failed');
        }
    }
    $job;
}

#|( Per-frame pump: release/supersede held jobs whose dependencies
    settled, tick every queue, prune terminal bookkeeping. )
method tick(--> Nil) {
    my @held = $!lock.protect: {
        %!jobs.values.grep({ $_<held> && $_<job>.completion.status !~~ Kept })
            .map(*<job>).List;
    };
    for @held -> $job {
        given self!dep-status($job) {
            when 'ready' {
                $!lock.protect: { %!jobs{$job.id}<held> = False with %!jobs{$job.id} };
                self!release($job, self.queue($job.queue-name));
            }
            when 'doomed' {
                $!lock.protect: { %!jobs{$job.id}<held> = False with %!jobs{$job.id} };
                self!supersede-held($job, reason => 'dep-failed');
            }
            default {
                my $held-at = $!lock.protect: { %!jobs{$job.id}<held-at> };
                if $held-at.defined && now - $held-at > $!held-warn-after
                   && !%!warned-held{$job.id} {
                    %!warned-held{$job.id} = True;
                    $.log.error('coordinator/job-held-too-long',
                        job-id => $job.id, kind => ($job.?kind // ''),
                        depends-on => $job.depends-on.join(','),
                        after => $job.after.join(','));
                }
            }
        }
    }

    .tick for $!lock.protect({ %!queues.values.List });

    # Prune: terminal jobs with no live dependents. Materialize the
    # candidate list BEFORE deleting — a lazy grep over .keys while
    # the hash shrinks is the classic mutation-under-iteration trap.
    $!lock.protect: {
        my @terminal-ids = %!jobs.keys.grep({
            %!jobs{$_}<job>.completion.status ~~ Kept
        }).List;
        for @terminal-ids -> $id {
            my Bool $has-live-dependent = so (%!dependents{$id} // []).first(
                -> $did {
                    my $e = %!jobs{$did};
                    $e.defined && $e<job>.completion.status !~~ Kept;
                });
            unless $has-live-dependent {
                %!jobs{$id}:delete;
                %!dependents{$id}:delete;
                %!warned-held{$id}:delete;
            }
        }
    }
}

#|( Supersede a job (held, queued, or running) and cascade over hard
    dependents. System-initiated: the ledger records grey
    'superseded', never a red retryable row. )
method supersede(Str:D $job-id, Str :$reason = 'superseded' --> Bool) {
    my %entry = $!lock.protect({ (%!jobs{$job-id} // {}).Hash });
    return False unless %entry;
    my $job = %entry<job>;
    my Bool $did;
    if %entry<held> && $job.completion.status !~~ Kept {
        $!lock.protect: { %!jobs{$job-id}<held> = False with %!jobs{$job-id} };
        $did = self!supersede-held($job, :$reason);
    } else {
        my $queue = self.queue(%entry<queue-name>);
        $did = $queue.defined
            ?? $queue.cancel($job-id, outcome => 'superseded')
            !! False;
    }
    self!cascade-supersede($job-id, :$reason);
    $did;
}

#|( Cancel a job wherever it lives. Held jobs are superseded (their
    replacement semantics are the caller's problem); queued/running
    route to the owning queue with the caller's on-cancel. A job the
    coordinator never saw is looked for in every registered queue.
    Hard dependents cascade to superseded. )
method cancel(Str:D $job-id, :&on-cancel --> Bool) {
    my %entry = $!lock.protect({ (%!jobs{$job-id} // {}).Hash });
    my Bool $did = False;
    if %entry {
        if %entry<held> && %entry<job>.completion.status !~~ Kept {
            $!lock.protect: { %!jobs{$job-id}<held> = False with %!jobs{$job-id} };
            $did = self!supersede-held(%entry<job>, reason => 'cancelled');
        } else {
            my $queue = self.queue(%entry<queue-name>);
            $did = $queue.defined ?? $queue.cancel($job-id, :&on-cancel) !! False;
        }
    } else {
        # Not coordinator-submitted — try every queue directly.
        for $!lock.protect({ %!queues.values.List }) -> $q {
            if $q.find-job($job-id).defined {
                $did = $q.cancel($job-id, :&on-cancel);
                last;
            }
        }
    }
    self!cascade-supersede($job-id, reason => 'dep-cancelled');
    $did;
}

#|( Look up a job by id: coordinator bookkeeping first, then every
    registered queue. Returns the Job or Nil. )
method find-job(Str:D $job-id) {
    my %entry = $!lock.protect({ (%!jobs{$job-id} // {}).Hash });
    return %entry<job> if %entry;
    for $!lock.protect({ %!queues.values.List }) -> $q {
        my $found = $q.find-job($job-id);
        return $found with $found;
    }
    Nil;
}

#|( Every live (non-terminal) job matching the given criteria, held
    and queued/running alike.

    C<:$scope-id> filters on the job's scope with C<eqv> (so Int and
    Str scopes both work). C<:&where> is an arbitrary predicate
    applied to each surviving job — the escape hatch for
    domain-specific fields the coordinator can't know about:

    =begin code :lang<raku>
    my @stale = $coord.find-jobs(
        scope-id => $conversation-id,
        where    => -> $j { $j ~~ RenderJob && $j.message-id == $msg-id },
    );
    =end code

    Both are optional; with neither, you get every live job. )
method find-jobs(:$scope-id, :&where --> Seq) {
    my @all = $!lock.protect: {
        %!jobs.values.map(*<job>).grep({ .completion.status !~~ Kept }).List;
    };
    @all.grep({
        (!$scope-id.defined || .scope-id eqv $scope-id)
        && (!&where.defined || ?&where($_))
    }).Seq;
}

#|( Ask each registered queue, in registration order, to cancel the
    most recent job in C<$scope>; stops at the first that finds one. )
method cancel-most-recent-in-scope($scope, :&on-cancel --> Bool) {
    for $!lock.protect({ %!queues.values.List }) -> $q {
        return True if $q.cancel-most-recent-in-scope($scope, :&on-cancel);
    }
    False;
}

#|( Held + pending + LIVE running across every queue. Uses the
    queues' C<live-count>, not C<total-count>: a cancelled/superseded
    job still draining an unabortable call occupies the backend but
    is nothing the user waits on — it must not hold an input locked
    (a new submission simply pends behind the drain). )
method total-active(--> Int) {
    my Int $held = $!lock.protect: {
        %!jobs.values.grep({
            $_<held> && $_<job>.completion.status !~~ Kept
        }).elems;
    };
    my Int $queued = [+] $!lock.protect({ %!queues.values.List }).map(*.live-count);
    $held + ($queued // 0);
}

#|( How many live coordinator-tracked jobs advertise C<$kind>. Probed
    rather than typechecked (C<.?kind>), so jobs without the concept
    simply never match. )
method active-count(Str:D $kind --> Int) {
    my @live = $!lock.protect: {
        %!jobs.values.grep({ $_<job>.completion.status !~~ Kept }).map(*<job>).List;
    };
    @live.grep({ (.?kind // '') eq $kind }).elems;
}

# --- internals -------------------------------------------------------------

#| Keys of a dep-results entry that the coordinator owns outright.
my constant RESERVED-DEP-KEYS = Set.new(<state result>);

#| 'ready' | 'blocked' | 'doomed'
method !dep-status(Job:D $job --> Str) {
    for $job.depends-on -> $dep-id {
        my $dep = $!lock.protect({ (%!jobs{$dep-id} // {})<job> });
        without $dep {
            $.log.error('coordinator/unknown-dependency',
                job-id => $job.id, dep-id => $dep-id);
            return 'doomed';
        }
        return 'blocked' unless $dep.completion.status ~~ Kept;
        return 'doomed' unless $dep.state eq 'done';
    }
    for $job.after -> $dep-id {
        my $dep = $!lock.protect({ (%!jobs{$dep-id} // {})<job> });
        # Unknown soft dep: run anyway — soft edges are best-effort.
        next without $dep;
        return 'blocked' unless $dep.completion.status ~~ Kept;
    }
    'ready';
}

method !release(Job:D $job, $queue --> Nil) {
    for flat $job.depends-on, $job.after -> $dep-id {
        my $dep = $!lock.protect({ (%!jobs{$dep-id} // {})<job> });
        next without $dep;
        my %r = state => $dep.state, result => $dep.result;
        my %extras = $dep.dep-extras;
        for %extras.kv -> $k, $v {
            %r{$k} = $v unless RESERVED-DEP-KEYS{$k};
        }
        $job.dep-results{$dep-id} = %r;
    }
    $queue.enqueue($job);
}

method !dispatch-waiting(Job:D $job, $queue --> Nil) {
    return without $.store;
    $.store.dispatch('queue/job-waiting',
        queue => $job.queue-name,
        job   => $queue.job-snapshot($job),
    );
}

method !supersede-held(Job:D $job, Str :$reason! --> Bool) {
    $job.request-cancel;
    my Bool $won = $job.finish('superseded', :by<cancel>);
    if $won {
        $.log.info('coordinator/job-superseded-held',
            job-id => $job.id, kind => ($job.?kind // ''), :$reason);
        with $.store {
            my $queue = self.queue($job.queue-name);
            .dispatch('queue/job-superseded',
                queue => $job.queue-name,
                job   => ($queue.defined
                    ?? $queue.job-snapshot($job)
                    !! %( id => $job.id, job-id => $job.id,
                          kind => ($job.?kind // ''),
                          scope-id => $job.scope-id,
                          state => $job.state )),
            );
        }
    }
    $won;
}

#| Transitively supersede every HARD dependent of $job-id.
method !cascade-supersede(Str:D $job-id, Str :$reason! --> Nil) {
    my @dependent-ids = $!lock.protect({ (%!dependents{$job-id} // []).List });
    for @dependent-ids -> $did {
        my %entry = $!lock.protect({ (%!jobs{$did} // {}).Hash });
        next unless %entry;
        my $dep-job = %entry<job>;
        next if $dep-job.completion.status ~~ Kept;
        # Soft (after) dependents run regardless of the dep's fate.
        next unless $job-id ∈ $dep-job.depends-on;
        if %entry<held> {
            $!lock.protect: { %!jobs{$did}<held> = False with %!jobs{$did} };
            self!supersede-held($dep-job, :$reason);
        } else {
            my $queue = self.queue(%entry<queue-name>);
            $queue.cancel($did, outcome => 'superseded') if $queue.defined;
        }
        self!cascade-supersede($did, :$reason);
    }
}
