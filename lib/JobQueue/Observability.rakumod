=begin pod

=head1 NAME

JobQueue::Observability - null log / tracer objects and the shapes real ones must match

=head1 DESCRIPTION

C<JobQueue::Queue> and C<JobQueue::Coordinator> emit structured log
lines and tracing spans, but they must not drag a logging framework
into your dependency graph. Both therefore take a C<log> and a
C<tracer> object by injection, defaulting to the silent null objects
in this module.

The null objects are also the specification: anything you inject has
to answer the same calls.

=head2 The log contract

Three verbs, all C<Nil>-returning, all with the same shape — a short
event name and a bag of structured fields:

=begin code :lang<raku>

method debug(Str:D $verb, *%fields --> Nil) { }
method info( Str:D $verb, *%fields --> Nil) { }
method error(Str:D $verb, *%fields --> Nil) { }

=end code

The queue and coordinator name events C<"<queue-name>/job-started">,
C<'coordinator/unknown-dependency'> and so on, so a grep-able log
falls out for free. Fields are always simple scalars (ids, outcome
strings, counts) — never Promises or job objects.

An adapter over an existing logger is three lines:

=begin code :lang<raku>

class MyLogAdapter {
    method debug(Str:D $verb, *%fields --> Nil) { $*LOG.debug("$verb {%fields.sort.map({ .key ~ '=' ~ .value }).join(' ')}") }
    method info( Str:D $verb, *%fields --> Nil) { $*LOG.info( "$verb {%fields.sort.map({ .key ~ '=' ~ .value }).join(' ')}") }
    method error(Str:D $verb, *%fields --> Nil) { $*LOG.error("$verb {%fields.sort.map({ .key ~ '=' ~ .value }).join(' ')}") }
}

my $queue = JobQueue::Queue.new(:$name, :&is-parallel-now, :&run-job,
                                log => MyLogAdapter.new);

=end code

=head2 The tracer contract

Three calls, modelled on Chrome-trace style span recording:

=begin code :lang<raku>

method enabled(--> Bool)                       # cheap gate
method start(Str:D $name, *%opts)              # returns a span object
method instant(Str:D $name, *%opts --> Nil)    # zero-duration marker

=end code

The returned span answers C<finish(*%args)>. Named options passed to
C<start> and C<instant> are C<cat> (category string), C<args> (a Hash
of trace arguments) and C<slow-ms> (a Numeric slow-path threshold) —
a tracer is free to ignore any of them.

The queue B<always> gates C<start> behind C<enabled>, so a disabled
tracer costs one method call per operation and nothing else. It never
gates C<instant> — a tracer that cares must check for itself, exactly
as C<NullTracer> does by doing nothing.

C<Selkie::Trace> matches this contract as-is; wrapping it takes an
adapter only because its methods are class methods:

=begin code :lang<raku>

class SelkieTracer {
    method enabled(--> Bool)     { Selkie::Trace.enabled }
    method start(Str:D $n, *%o)  { Selkie::Trace.start($n, |%o) }
    method instant(Str:D $n, *%o --> Nil) { Selkie::Trace.instant($n, |%o) }
}

=end code

=head1 EXPORTS

C<NullLog>, C<NullTracer> and C<NullSpan> are exported by default.

=end pod

unit module JobQueue::Observability;

#|( The span handed back by C<NullTracer.start>. Exists so callers
    that don't gate on C<enabled> still get a live object to
    C<finish>, rather than a Nil they'd have to guard. )
class NullSpan is export {
    method finish(*%args --> Nil) { }
}

#|( Silent tracer. C<enabled> is a constant False, so every
    C<enabled ?? start !! Nil> site in the queue short-circuits to Nil
    and no span object is ever allocated on the null path. )
class NullTracer is export {
    method enabled(--> False) { }
    method start(Str:D $name, *%opts --> NullSpan) { NullSpan.new }
    method instant(Str:D $name, *%opts --> Nil) { }
}

#| Silent log. Every verb is a no-op that discards its fields.
class NullLog is export {
    method debug(Str:D $verb, *%fields --> Nil) { }
    method info(Str:D $verb, *%fields --> Nil) { }
    method error(Str:D $verb, *%fields --> Nil) { }
}
