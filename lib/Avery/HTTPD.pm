package Avery::HTTPD;

use strict;
use warnings;
use v5.10;
use utf8;

use AnyEvent;
use AnyEvent::HTTPD;
use Avery::Model::DB;
use Cpanel::JSON::XS;
use Data::Dumper;
use Encode qw(decode_utf8);
use HTTP::Server::Simple::CGI;
use IO::Pipe;
use IPC::ShareLite;
use List::Util qw(min);
use Memory::Usage;
use Mojo::Log;
use Time::HiRes qw( gettimeofday tv_interval usleep );

my $httpd = AnyEvent::HTTPD->new( port => 80, backlog => 100000 );

my %entities = ( users => 1, visits => 1, locations => 1 );

my $logger = Mojo::Log->new;

my $db = Avery::Model::DB->new( logger => $logger );
$db->load();

my $JSON = Cpanel::JSON::XS->new->utf8;

my $STAGE = 1;

my %FORKS;

my $SHARE;
my %SHARES = (
  1 => IPC::ShareLite->new(
    -key     => 1,
    -create  => 'yes',
    -destroy => 'yes',
  ),
  2 => IPC::ShareLite->new(
    -key     => 2,
    -create  => 'yes',
    -destroy => 'yes',
  )
);

my $mu = Memory::Usage->new();
$mu->record('starting work');

sub run {
  $httpd->reg_cb(
    '' => sub {
      my ( $httpd, $req ) = @_;
      my $q = {
        data => {
          method  => $req->method,
          path    => $req->url->path,
          content => $req->content,
          vars    => { $req->vars },
        },
        resp => sub {
          $req->respond( $_[0] );
        },
      };
      handle_request($q);
    }
  );

  $httpd->run;
}

sub handle_request {
  my $q = shift;

  if ( $q->{data}{method} ne 'POST' && $STAGE == 2 ) {
    $logger->info( $mu->report );
    $STAGE = 3;
    my $forked = _fork();
    if ($forked) {
      _to_worker($q);
    }
  }
  elsif ( $STAGE == 3 ) {
    _to_worker($q);
  }
  else {
    _process($q);
  }

  return;
}

sub _process {
  my $q = shift;

  my @path = split '/', $q->{data}{path};

  if ( scalar(@path) == 3
    && $entities{ $path[1] }
    && $path[2] eq 'new'
    && $q->{data}{method} eq 'POST' )
  {
    $STAGE = 2;

    my $data = $q->{data}{content};
    my $val = eval { $JSON->decode($data) };

    unless ( $val && keys %$val ) {
      _400($q);
      return;
    }
    my $status = $db->create( $path[1], $val );

    if ( $status == 1 ) {
      _200( $q, '{}' );
    }
    elsif ( $status == -2 ) {
      _400($q);
    }
  }
  elsif ( scalar(@path) == 3
    && $entities{ $path[1] }
    && $path[2] =~ m/^\d+$/ )
  {
    if ( $q->{data}{method} eq 'GET' ) {
      my $val = $db->read( $path[1], $path[2] );

      unless ($val) {
        _404($q);
        return;
      }

      _200( $q, $JSON->encode($val) );
    }
    elsif ( $q->{data}{method} eq 'POST' ) {
      $STAGE = 2;

      my $data = $q->{data}{content};
      my $val = eval { $JSON->decode($data) };

      unless ( $val && keys %$val ) {
        _400($q);
        return;
      }

      my $status = $db->update( $path[1], $path[2], $val );

      if ( $status == 1 ) {
        _200( $q, '{}' );
      }
      elsif ( $status == -1 ) {
        _404($q);
      }
      elsif ( $status == -2 ) {
        _400($q);
      }
    }
    else {
      _404($q);
    }
  }
  elsif ( scalar(@path) == 4
    && $path[1] eq 'users'
    && $path[2] =~ m/^\d+$/
    && $path[3] eq 'visits'
    && $q->{data}{method} eq 'GET' )
  {
    my %args;
    foreach (qw( fromDate toDate country toDistance )) {
      next unless defined $q->{data}{vars}{$_};
      $args{$_} = $q->{data}{vars}{$_};
      $args{$_} = decode_utf8( $args{$_} ) if $_ eq 'country';
    }

    my $vals = $db->users_visits( $path[2], %args );

    if ( $vals == -1 ) {
      _404($q);
    }
    elsif ( $vals == -2 ) {
      _400($q);
    }
    else {
      _200( $q, $JSON->encode( { visits => $vals } ) );
    }
  }
  elsif ( scalar(@path) == 4
    && $path[1] eq 'locations'
    && $path[2] =~ m/^\d+$/
    && $path[3] eq 'avg'
    && $q->{data}{method} eq 'GET' )
  {
    my %args;
    foreach (qw( fromDate toDate fromAge toAge gender )) {
      next unless defined $q->{data}{vars}{$_};
      $args{$_} = $q->{data}{vars}{$_};
    }

    my $t0 = [gettimeofday];

    my $avg = $db->avg( $path[2], %args );

    my $t1 = tv_interval($t0);

    if ( $avg == -1 ) {
      _404($q);
    }
    elsif ( $avg == -2 ) {
      _400($q);
    }
    else {
      _200( $q, $JSON->encode( { avg => $avg } ) );
    }
  }
  else {
    _404($q);
  }

  return;
}

sub _to_worker {
  my $q = shift;

  my $min = min map { $_->{count} } values %FORKS;

  foreach my $fork ( keys %FORKS ) {
    _check($fork);
  }

  foreach my $fork ( keys %FORKS ) {
    next if $FORKS{$fork}->{count} > $min;
    $FORKS{$fork}->{count}++;

    my $pipe = $FORKS{$fork}->{pipe};
    my $enc  = $JSON->encode( $q->{data} );
    print $pipe "$enc\n";

    push @{ $FORKS{$fork}->{qu} }, $q->{resp};

    last;
  }

  return;
}

sub _200 {
  my ( $q, $data ) = @_;

  unless ( $q->{resp} ) {
    $SHARE->lock(IPC::ShareLite::LOCK_EX);
    my $val = $SHARE->fetch;
    if ($val) {
      $val = $JSON->decode($val);
    }
    else {
      $val = [];
    }
    push @$val, { code => 200, data => $data };
    $SHARE->store( $JSON->encode($val) );
    $SHARE->unlock();
    return;
  }

  $q->{resp}->(
    [ 200, 'OK', { 'Content-Type' => 'application/json;charset=UTF-8' },
      $data
    ]
  );

  return;
}

sub _404 {
  my $q = shift;

  unless ( $q->{resp} ) {
    $SHARE->lock(IPC::ShareLite::LOCK_EX);
    my $val = $SHARE->fetch;
    if ($val) {
      $val = $JSON->decode($val);
    }
    else {
      $val = [];
    }
    push @$val, { code => 404 };
    $SHARE->store( $JSON->encode($val) );
    $SHARE->unlock();
    return;
  }

  $q->{resp}->(
    [ 404, 'OK', { 'Content-Type' => 'application/json;charset=UTF-8' }, '{}'
    ]
  );

  return;
}

sub _400 {
  my $q = shift;

  unless ( $q->{resp} ) {
    $SHARE->lock(IPC::ShareLite::LOCK_EX);
    my $val = $SHARE->fetch;
    if ($val) {
      $val = $JSON->decode($val);
    }
    else {
      $val = [];
    }
    push @$val, { code => 400 };
    $SHARE->store( $JSON->encode($val) );
    $SHARE->unlock();
    return;
  }

  $q->{resp}->(
    [ 400, 'OK', { 'Content-Type' => 'application/json;charset=UTF-8' }, '{}'
    ]
  );

  return;
}

sub _fork {
  for my $i ( 1 .. 2 ) {
    my $pipe = IO::Pipe->new();
    my $pid;

    if ( $pid = fork() ) {
      $pipe->writer();
      $pipe->autoflush(1);

      my $timer = AE::timer 0.1, 0.1, sub {
        _check($i);
      };

      $FORKS{$i} = { count => 0, pipe => $pipe, timer => $timer };

      usleep 1;
    }
    elsif ( defined $pid ) {
      $logger->info("Forked $$");
      $pipe->reader();
      $SHARE = $SHARES{$i};

      while (<$pipe>) {
        my $q = { data => $JSON->decode($_) };
        _process($q);
      }

      return;
    }
  }

  return 1;
}

sub _check {
  my $i = shift;

  my $val = $SHARES{$i}->fetch;
  return unless $val;

  $SHARES{$i}->lock(IPC::ShareLite::LOCK_EX);
  $val = $SHARES{$i}->fetch;
  $SHARES{$i}->store('[]');
  $SHARES{$i}->unlock();

  my $dec = $JSON->decode($val);
  return unless $dec && scalar @$dec;

  foreach my $item (@$dec) {
    $item->{data} //= '{}';

    my $resp = shift @{ $FORKS{$i}->{qu} };
    $resp->(
      [ $item->{code}, 'OK',
        { 'Content-Type' => 'application/json;charset=UTF-8' },
        $item->{data}
      ]
    );
  }

  return;
}

1;
