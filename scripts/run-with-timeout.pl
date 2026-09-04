#!/usr/bin/perl

use strict;
use warnings;
use POSIX qw(:sys_wait_h setpgid WIFEXITED WEXITSTATUS WIFSIGNALED WTERMSIG);
use Time::HiRes qw(clock_gettime sleep CLOCK_MONOTONIC);

sub usage {
    print STDERR "usage: scripts/run-with-timeout.pl HARD_LIMIT_SECONDS -- COMMAND [ARG ...]\n";
    exit 2;
}

@ARGV >= 3 or usage();
my $hard_limit = shift @ARGV;
$hard_limit =~ /\A[1-9][0-9]*\z/ or usage();
shift(@ARGV) eq '--' or usage();
@ARGV or usage();

my $grace = exists $ENV{OVERSYNC_TIMEOUT_GRACE_SECONDS}
    ? $ENV{OVERSYNC_TIMEOUT_GRACE_SECONDS}
    : 10;
$grace =~ /\A(?:0|[1-9][0-9]*)\z/ or usage();
$grace <= $hard_limit or usage();

my @command = @ARGV;
my $child = fork();
defined $child or die "fork failed: $!\n";
if ($child == 0) {
    setpgid(0, 0) or die "setpgid failed: $!\n";
    exec { $command[0] } @command;
    die "exec $command[0] failed: $!\n";
}

# Close the fork/setpgid race from the parent side as well. EACCES means the
# child already exec'd after establishing its own group.
eval { setpgid($child, $child) };

my $forwarded_signal = 0;
for my $name (qw(HUP INT TERM)) {
    my $number = $name eq 'HUP' ? 1 : $name eq 'INT' ? 2 : 15;
    $SIG{$name} = sub {
        $forwarded_signal ||= $number;
        kill $name, -$child;
    };
}

my $started = clock_gettime(CLOCK_MONOTONIC);
my $term_at = $started + $hard_limit - $grace;
my $kill_at = $started + $hard_limit;
my $term_sent = 0;
my $timed_out = 0;
my $status;

while (1) {
    my $waited = waitpid($child, WNOHANG);
    if ($waited == $child) {
        $status = $?;
        last;
    }
    die "waitpid failed: $!\n" if $waited < 0;

    my $now = clock_gettime(CLOCK_MONOTONIC);
    if (!$term_sent && $now >= $term_at) {
        $timed_out = 1;
        $term_sent = 1;
        kill 'TERM', -$child;
    }
    if ($now >= $kill_at) {
        $timed_out = 1;
        kill 'KILL', -$child;
        waitpid($child, 0);
        $status = $?;
        last;
    }
    sleep 0.02;
}

# A command can exit while leaving background descendants in its process
# group. Terminate that group before reporting the leader's status.
if (kill 0, -$child) {
    kill 'TERM', -$child;
    my $cleanup_until = clock_gettime(CLOCK_MONOTONIC) + ($grace > 0 ? $grace : 0.05);
    $cleanup_until = $kill_at if $cleanup_until > $kill_at;
    while ((kill 0, -$child) && clock_gettime(CLOCK_MONOTONIC) < $cleanup_until) {
        sleep 0.02;
    }
    $timed_out = 1 if clock_gettime(CLOCK_MONOTONIC) >= $kill_at && (kill 0, -$child);
    kill 'KILL', -$child if kill 0, -$child;
}

exit 124 if $timed_out;
exit 128 + $forwarded_signal if $forwarded_signal;
exit WEXITSTATUS($status) if WIFEXITED($status);
exit 128 + WTERMSIG($status) if WIFSIGNALED($status);
exit 125;
