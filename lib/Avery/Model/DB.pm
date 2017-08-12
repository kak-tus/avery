package Avery::Model::DB;

use strict;
use warnings;
use v5.10;
use utf8;

use Cpanel::JSON::XS;
use Data::Dumper;
use Encode qw(encode_utf8);
use List::Util qw(any);
use RedisDB;

my $JSON = Cpanel::JSON::XS->new->utf8;
my $REDIS;
my %LOCATIONS;

my %VALIDATION = (
  id         => { min => 1, max => 2147483647 },
  email      => { len => 100 },
  first_name => { len => 50 },
  last_name  => { len => 50 },
  gender  => { in  => { m => 1, f => 1 } },
  country => { len => 50 },
  city    => { len => 50 },
  id         => { min => 0,         max => 2147483647 },
  location   => { min => 0,         max => 2147483647 },
  user       => { min => 0,         max => 2147483647 },
  visited_at => { min => 946684800, max => 1420156799 },
  mark       => { min => 0,         max => 5 },
  fromDate   => { min => 0,         max => 2147483647, optional => 1 },
  toDate     => { min => 0,         max => 2147483647, optional => 1 },
  toDistance => { min => 0,         max => 2147483647, optional => 1 },
);

sub new {
  $REDIS = RedisDB->new( path => '/var/run/redis/redis.sock' );

  return bless {};
}

sub load {
  my $self = shift;

  my @files = glob '/tmp/unzip/users*.json';
  push @files, glob '/tmp/unzip/locations*.json';
  push @files, glob '/tmp/unzip/visits*.json';

  use Time::HiRes;

  foreach my $file (@files) {
    say $file;
    say Time::HiRes::time;

    open my $fl, "$file";
    my $st = <$fl>;
    close $fl;

    my $decoded = $JSON->decode($st);

    my $entity = ( keys %$decoded )[0];
    say $entity;

    foreach my $val ( @{ $decoded->{$entity} } ) {
      my $status = $self->create( $entity, $val );
    }
    say Time::HiRes::time;
  }

  $REDIS->mainloop;

  say 'Loaded';
  say Time::HiRes::time;

  return;
}

sub create {
  my $self = shift;
  my ( $entity, $val ) = @_;

  foreach my $key ( keys %$val ) {
    next unless $VALIDATION{$key};
    return -2 if _validate( 'create', $key, $val->{$key} ) == -2;
  }

  my $encoded = $JSON->encode($val);
  $REDIS->set( 'val_' . $entity . '_' . $val->{id}, $encoded, sub { } );

  if ( $entity eq 'locations' ) {
    $LOCATIONS{ $val->{id} } = $val;
  }

  if ( $entity eq 'visits' ) {
    my $location = $LOCATIONS{ $val->{location} };

    $REDIS->zadd( 'val_users_visits_' . $val->{user},
      $val->{visited_at}, $encoded, sub { } );

    $REDIS->sadd(
      'val_countries_locations_' . encode_utf8( $location->{country} ),
      $encoded, sub { } );

    $REDIS->zadd( 'val_users_distances_locations_' . $val->{user},
      $location->{distance}, $encoded, sub { } );

    $REDIS->sadd( 'val_users_locations_' . $val->{user}, $encoded, sub { } );
  }

  return 1;
}

sub read {
  my $self = shift;
  my ( $entity, $id ) = @_;

  my $val = $REDIS->get( 'val_' . $entity . '_' . $id );

  return $val;
}

sub update {
  my $self = shift;
  my ( $entity, $id, $val ) = @_;

  my $curr = $REDIS->get( 'val_' . $entity . '_' . $id );
  return -1 unless $curr;

  my $decoded = $JSON->decode($curr);
  foreach my $key ( keys %$val ) {
    if ( $VALIDATION{$key} ) {
      return -2 if _validate( 'update', $key, $val->{$key} ) == -2;
    }

    $decoded->{$key} = $val->{$key};
  }

  my $encoded = $JSON->encode($decoded);
  $REDIS->set( 'val_' . $entity . '_' . $id, $encoded );

  return 1;
}

sub _validate {
  my ( $action, $key, $val ) = @_;

  if ( $VALIDATION{$key}->{len} ) {
    if ( defined($val)
      && length($val) > $VALIDATION{$key}->{len} )
    {
      ## say "fail $action key:$key";
      return -2;
    }
  }
  elsif ( $VALIDATION{$key}->{in} ) {
    if ( defined($val) && !$VALIDATION{$key}->{in}->{$val} ) {
      ## say "fail $action key:$key";
      return -2;
    }
  }
  elsif ( $VALIDATION{$key}->{max} ) {
    return 1 if $VALIDATION{$key}->{optional} && !defined($val);
    if ( !defined($val)
      || $val !~ m/^\-{0,1}\d+$/
      || $val < $VALIDATION{$key}->{min}
      || $val > $VALIDATION{$key}->{max} )
    {
      ## say "fail $action key:$key";
      return -2;
    }
  }

  return 1;
}

sub users_visits {
  my $self   = shift;
  my $id     = shift;
  my %params = @_;

  foreach my $key ( keys %params ) {
    next unless $VALIDATION{$key};
    return -2 if _validate( 'users_visits', $key, $params{$key} ) == -2;
  }

  my $from = $params{fromDate} // 0;
  my $to   = $params{toDate}   // 2147483647;

  my $vals
      = $REDIS->zrangebyscore( 'val_users_visits_' . $id, "($from", "($to" );
  return -1 unless $vals;

  my @res;

  my $locations;
  if ( defined $params{country} ) {
    my $locations_enc = $REDIS->smembers(
      'val_countries_locations_' . encode_utf8( $params{country} ) );
    $locations = [ map { $JSON->decode($_) } @$locations_enc ];
  }

  my $dist_locations;
  if ( defined $params{toDistance} ) {
    my $dist_locations_enc
        = $REDIS->zrangebyscore( 'val_users_distances_locations_' . $id,
      0, '(' . $params{toDistance} );
    $dist_locations = [ map { $JSON->decode($_) } @$dist_locations_enc ];
  }

  my $list = $locations || $dist_locations;
  unless ($list) {
    my $list_enc = $REDIS->smembers( 'val_users_locations_' . $id );
    $list = [ map { $JSON->decode($_) } @$list_enc ];
  }

  my %locations_list = map { ( $_->{id} => $_ ) } @$list;

  foreach my $val (@$vals) {
    my $decoded = $JSON->decode($val);

    if ( defined $params{country} ) {
      next
          unless ( any { $decoded->{location} == $_->{id} } @$locations );
    }

    if ( defined $params{toDistance} ) {
      next
          unless (
        any { $decoded->{location} == $_->{id} }
        @$dist_locations
          );
    }

    my %visit = (
      mark       => $decoded->{mark},
      visited_at => $decoded->{visited_at},
      place      => $locations_list{ $decoded->{location} }->{place},
    );
    push @res, \%visit;
  }

  return \@res;
}

1;
