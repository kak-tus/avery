package Avery::HTTP;

use strict;
use warnings;
use v5.10;
use utf8;

use base qw(HTTP::Server::Simple::CGI);

use Avery::Model::DB;
use Cpanel::JSON::XS;
use Encode qw(decode_utf8);
use HTTP::Server::Simple::CGI;
use IO::Pipe;
use Mojo::Log;
use Time::HiRes qw( gettimeofday tv_interval );

my %entities = ( users => 1, visits => 1, locations => 1 );

my $logger = Mojo::Log->new;

my $db = Avery::Model::DB->new( logger => $logger );
$db->load();

my $JSON = Cpanel::JSON::XS->new->utf8;

my $STAGE = 1;

my %FORKS = ( 0 => { count => 0 } );
my $CHILD_PIPER;
my $CHILD_PIPEW;

sub handle_request {
  my $self = shift;
  my $q    = shift;

  if ( $q->request_method ne 'POST' && $STAGE == 2 ) {
    $STAGE = 3;
    my $forked = $self->_fork();
    if ($forked) {
      $self->_to_worker($q);
    }
  }
  else {
    $self->_process($q);
  }

  return;
}

sub _process {
  my $self = shift;
  my $q    = shift;

  my @path = split '/', $q->path_info;

  if ( scalar(@path) == 3
    && $entities{ $path[1] }
    && $path[2] eq 'new'
    && $q->request_method eq 'POST' )
  {
    $STAGE = 2;

    my $data = $q->param('POSTDATA');
    my $val = eval { $JSON->decode($data) };

    unless ( $val && keys %$val ) {
      $self->_400($q);
      return;
    }
    my $status = $db->create( $path[1], $val );

    if ( $status == 1 ) {
      $self->_200($q);
      print '{}';
    }
    elsif ( $status == -2 ) {
      $self->_400($q);
    }
  }
  elsif ( scalar(@path) == 3
    && $entities{ $path[1] }
    && $path[2] =~ m/^\d+$/ )
  {
    if ( $q->request_method eq 'GET' ) {
      my $val = $db->read( $path[1], $path[2] );

      unless ($val) {
        $self->_404($q);
        return;
      }

      $self->_200($q);
      print $JSON->encode($val);
    }
    elsif ( $q->request_method eq 'POST' ) {
      $STAGE = 2;

      my $data = $q->param('POSTDATA');
      my $val = eval { $JSON->decode($data) };

      unless ( $val && keys %$val ) {
        $self->_400($q);
        return;
      }

      my $status = $db->update( $path[1], $path[2], $val );

      if ( $status == 1 ) {
        $self->_200($q);
        print '{}';
      }
      elsif ( $status == -1 ) {
        $self->_404($q);
      }
      elsif ( $status == -2 ) {
        $self->_400($q);
      }
    }
    else {
      $self->_404($q);
    }
  }
  elsif ( scalar(@path) == 4
    && $path[1] eq 'users'
    && $path[2] =~ m/^\d+$/
    && $path[3] eq 'visits'
    && $q->request_method eq 'GET' )
  {
    my %args;
    foreach (qw( fromDate toDate country toDistance )) {
      next unless defined $q->param($_);
      $args{$_} = $q->param($_);
      $args{$_} = decode_utf8( $args{$_} ) if $_ eq 'country';
    }

    my $vals = $db->users_visits( $path[2], %args );

    if ( $vals == -1 ) {
      $self->_404($q);
    }
    elsif ( $vals == -2 ) {
      $self->_400($q);
    }
    else {
      $self->_200($q);
      print $JSON->encode( { visits => $vals } );
    }
  }
  elsif ( scalar(@path) == 4
    && $path[1] eq 'locations'
    && $path[2] =~ m/^\d+$/
    && $path[3] eq 'avg'
    && $q->request_method eq 'GET' )
  {
    my %args;
    foreach (qw( fromDate toDate fromAge toAge gender )) {
      next unless defined $q->param($_);
      $args{$_} = $q->param($_);
    }

    my $t0 = [gettimeofday];

    my $avg = $db->avg( $path[2], %args );

    my $t1 = tv_interval($t0);
    $logger->info("Avg: $t1");

    if ( $avg == -1 ) {
      $self->_404($q);
    }
    elsif ( $avg == -2 ) {
      $self->_400($q);
    }
    else {
      $self->_200($q);
      print $JSON->encode( { avg => $avg } );
    }
  }
  else {
    $self->_404($q);
  }

  return;
}

sub _to_worker {
}

sub _200 {
  my $self = shift;
  my $q    = shift;

  print "HTTP/1.0 200 OK\r\n";
  print $q->header('application/json;charset=UTF-8');

  return;
}

sub _404 {
  my $self = shift;
  my $q    = shift;

  print "HTTP/1.0 404 OK\r\n";
  print $q->header('application/json;charset=UTF-8');
  print '{}';

  return;
}

sub _400 {
  my $self = shift;
  my $q    = shift;

  print "HTTP/1.0 400 OK\r\n";
  print $q->header('application/json;charset=UTF-8');
  print '{}';

  return;
}

sub _fork {
  my $self = shift;

  for my $i ( 1 .. 1 ) {
    my $pipe1 = IO::Pipe->new();
    my $pipe2 = IO::Pipe->new();
    my $pid;

    if ( $pid = fork() ) {
      $FORKS{$i} = { count => 0, pipew => $pipe1, piper => $pipe2 };
      $pipe1->writer();
      $pipe2->reader();
    }
    elsif ( defined $pid ) {
      $logger->info("Forked $$");
      $pipe1->reader();
      $pipe2->writer();
      $CHILD_PIPER = $pipe1;
      $CHILD_PIPEW = $pipe2;
      return;
    }
  }

  return 1;
}

1;
