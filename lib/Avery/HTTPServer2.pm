package Avery::HTTPServer2;

use strict;
use warnings;
use v5.10;
use utf8;

use AnyEvent;
use AnyEvent::HTTP::Server;
use Avery::Model::DB;
use Cpanel::JSON::XS;
use Data::Dumper;
use Encode qw(decode_utf8);
use EV;
use IO::Pipe;
use IPC::ShareLite;
use List::Util qw(min);
use Memory::Usage;
use Mojo::Log;
use Time::HiRes qw( gettimeofday tv_interval usleep );

my $httpd;

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
  ),
  3 => IPC::ShareLite->new(
    -key     => 3,
    -create  => 'yes',
    -destroy => 'yes',
  ),
);

my $mu = Memory::Usage->new();
$mu->record('starting work');

my %STAT;
my %CACHE;

sub run {
  $httpd = AnyEvent::HTTP::Server->new(
    host => '0.0.0.0',
    port => 80,
    cb   => sub {
      my $req = shift;

      if ( $req->method eq 'POST'
        && $req->headers->{'content-length'}
        && $req->headers->{'content-length'} > 0 )
      {
        return HANDLE => sub {
          my $h = $_[0];
          $h->on_read(
            sub {
              my $h = shift;
              _form_req( $req, $h->{rbuf} );
            }
          );
        };
      }
      else {
        _form_req($req);
      }

    }
  );

  $httpd->{backlog} = 5000;

  $httpd->listen;
  $httpd->accept;

  EV::loop;
}

sub _form_req {
  my ( $req, $content ) = @_;

  my $pos = index( $req->uri, '?' );
  $pos = length( $req->uri ) if $pos < 0;
  my $path = substr( $req->uri, 0, $pos );

  my $q = {
    data => {
      method  => $req->method,
      path    => $path,
      content => $content,
      vars    => $req->params,
    },
    resp => sub {
      my ( $status, $data ) = @_;
      $req->reply( $status, $data,
        { 'Content-Type' => 'application/json;charset=UTF-8' } );
    },
  };

  handle_request($q);

  return;
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

  my $key
      = $q->{data}{path} . '_'
      . join( '_',
    map { $_ . '_' . $q->{data}{vars}{$_} }
    sort keys %{ $q->{data}{vars} } );

  $STAT{$key} //= 0;
  $STAT{$key}++;

  if ( $CACHE{$key} ) {
    $q->{resp}->( $CACHE{$key}->{code}, $CACHE{$key}->{data} );
    return;
  }

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

    $q->{key} = $key;
    push @{ $FORKS{$fork}->{qu} }, $q;

    last;
  }

  return;
}

sub _200 {
  my ( $q, $data ) = @_;

  unless ( $q->{resp} ) {
    _store( 200, $data );
    return;
  }

  $q->{resp}->( 200, $data );

  return;
}

sub _404 {
  my $q = shift;

  unless ( $q->{resp} ) {
    _store( 404, '{}' );
    return;
  }

  $q->{resp}->( 404, '{}' );

  return;
}

sub _400 {
  my $q = shift;

  unless ( $q->{resp} ) {
    _store( 400, '{}' );
    return;
  }

  $q->{resp}->( 400, '{}' );

  return;
}

sub _store {
  my ( $code, $data ) = @_;

  $SHARE->lock(IPC::ShareLite::LOCK_EX);
  my $val = $SHARE->fetch;

  if ($val) {
    $val .= "\n";
  }
  else {
    $val = '';
  }

  $val .= "$code\n$data";
  $SHARE->store($val);
  $SHARE->unlock();

  return;
}

sub _fork {
  for my $i ( 1 .. 2 ) {
    my $pipe = IO::Pipe->new();
    my $pid;

    if ( $pid = fork() ) {
      $pipe->writer();
      $pipe->autoflush(1);

      my $timer = AE::timer 0.01, 0.01, sub {
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

  $Avery::Model::DB::DAT = undef;

  return 1;
}

sub _check {
  my $fork = shift;

  my $val = $SHARES{$fork}->fetch;
  return unless $val;

  $SHARES{$fork}->lock(IPC::ShareLite::LOCK_EX);
  $val = $SHARES{$fork}->fetch;
  $SHARES{$fork}->store('');
  $SHARES{$fork}->unlock();

  my @dec = split "\n", $val;
  return unless scalar @dec;

  for ( my $i = 0; $i < scalar(@dec) - 1; $i += 2 ) {
    my $q = shift @{ $FORKS{$fork}->{qu} };

    $q->{resp}->( $dec[$i], $dec[ $i + 1 ] );

    if ( $STAT{ $q->{key} } > 3 ) {
      $CACHE{ $q->{key} } = { code => $dec[$i], data => $dec[ $i + 1 ] };
    }
  }

  return;
}

1;
