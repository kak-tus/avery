package Avery::Model::DB;

use strict;
use warnings;
use v5.10;
use utf8;

use Clone qw(clone);
use Cpanel::JSON::XS;
use Data::Dumper;
use DateTime;
use Encode qw(encode_utf8);
use List::Util qw(any);
use RedisDB;
use Time::HiRes;

my $JSON      = Cpanel::JSON::XS->new->utf8;
my $JSON_SORT = Cpanel::JSON::XS->new->utf8->canonical(1);
my $REDIS;
my %LOCATIONS;
my %USERS;

my %VALIDATION = (
  id         => { min => 1, max => 2147483647 },
  email      => { len => 100 },
  first_name => { len => 50 },
  last_name  => { len => 50 },
  gender  => { in  => { m => 1, f => 1 } },
  country => { len => 50 },
  birth_date => { min => -1262304000, max => 915148800 },
  city       => { len => 50 },
  id         => { min => 0,           max => 2147483647 },
  location   => { min => 0,           max => 2147483647 },
  user       => { min => 0,           max => 2147483647 },
  distance   => { min => 0,           max => 2147483647 },
  visited_at => { min => 946684800,   max => 1420070400 },
  mark       => { min => 0,           max => 5 },
  fromDate   => { min => 0, max => 2147483647, optional => 1 },
  toDate     => { min => 0, max => 2147483647, optional => 1 },
  toDistance => { min => 0, max => 2147483647, optional => 1 },
  fromAge    => { min => 0, max => 2147483647, optional => 1 },
  toAge      => { min => 0, max => 2147483647, optional => 1 },
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

  my $start = Time::HiRes::time;
  say "Start $start";

  foreach my $file (@files) {
    open my $fl, "$file";
    my $st = <$fl>;
    close $fl;

    my $decoded = $JSON->decode($st);

    my $entity = ( keys %$decoded )[0];

    foreach my $val ( @{ $decoded->{$entity} } ) {
      my $status = $self->create( $entity, $val, without_validation => 1 );
    }
  }

  $REDIS->mainloop;

  undef %LOCATIONS;
  undef %USERS;

  my $end = Time::HiRes::time;
  say "Loaded $end, diff " . ( $end - $start );

  return;
}

sub create {
  my $self = shift;
  my ( $entity, $val ) = ( shift, shift );
  my %params = @_;

  if ( !$params{without_validation} ) {
    foreach my $key ( keys %$val ) {
      next unless $VALIDATION{$key};
      return -2 if _validate( 'create', $key, $val->{$key} ) == -2;
    }
  }

  my $encoded = $JSON->encode($val);
  $REDIS->set( 'val_' . $entity . '_' . $val->{id}, $encoded, sub { } );

  if ( $entity eq 'locations' ) {
    $LOCATIONS{ $val->{id} } = { hash => $val, encoded => $encoded };
  }
  elsif ( $entity eq 'users' ) {
    $USERS{ $val->{id} } = { hash => $val, encoded => $encoded };
  }

  if ( $entity eq 'visits' ) {
    my $location     = $LOCATIONS{ $val->{location} }->{hash};
    my $location_enc = $LOCATIONS{ $val->{location} }->{encoded};
    my $user         = $USERS{ $val->{user} };

    unless ($location) {
      $location_enc = $REDIS->get( 'val_locations_' . $val->{location} );
      $location     = $JSON->decode($location_enc);
    }

    unless ($user) {
      my $user_enc = $REDIS->get( 'val_users_' . $val->{user} );
      $user = { hash => $JSON->decode($user_enc), encoded => $user_enc };
    }

    my %user_visits_locations = ( visit => $val, location => $location );
    my $user_visits_locations_enc
        = $JSON_SORT->encode( \%user_visits_locations );

    $REDIS->zadd( 'val_users_visits_locations_' . $val->{user},
      $val->{visited_at}, $user_visits_locations_enc, sub { } );

    my %user_visit = ( user => $user->{hash}, visit => $val );
    my $user_visit_encoded = $JSON_SORT->encode( \%user_visit );

    $REDIS->zadd( 'val_locations_users_visits_' . $val->{location},
      $val->{visited_at}, $user_visit_encoded, sub { } );
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

  my $decoded      = $JSON->decode($curr);
  my $decoded_orig = clone($decoded);

  foreach my $key ( keys %$val ) {
    if ( $VALIDATION{$key} ) {
      return -2 if _validate( 'update', $key, $val->{$key} ) == -2;
    }

    $decoded->{$key} = $val->{$key};
  }

  my $encoded = $JSON->encode($decoded);
  $REDIS->set( 'val_' . $entity . '_' . $id, $encoded );

  if ( $entity eq 'visits' ) {
    my $user_orig_enc = $REDIS->get( 'val_users_' . $decoded_orig->{user} );
    my $user_orig     = {
      hash    => $JSON->decode($user_orig_enc),
      encoded => $user_orig_enc,
    };

    my $user_enc = $REDIS->get( 'val_users_' . $decoded->{user} );
    my $user = { hash => $JSON->decode($user_enc), encoded => $user_enc };

    my $location_orig_enc
        = $REDIS->get( 'val_locations_' . $decoded_orig->{location} );
    my $location_orig = $JSON->decode($location_orig_enc);

    my $location_enc = $REDIS->get( 'val_locations_' . $decoded->{location} );
    my $location     = $JSON->decode($location_enc);

    my %user_visits_locations_orig
        = ( visit => $decoded_orig, location => $location_orig );
    my $user_visits_locations_orig_enc
        = $JSON_SORT->encode( \%user_visits_locations_orig );

    $REDIS->zrem(
      'val_users_visits_locations_' . $decoded_orig->{user},
      $user_visits_locations_orig_enc,
      sub { }
    );

    my %user_visits_locations = ( visit => $decoded, location => $location );
    my $user_visits_locations_enc
        = $JSON_SORT->encode( \%user_visits_locations );

    $REDIS->zadd(
      'val_users_visits_locations_' . $decoded->{user},
      $decoded->{visited_at},
      $user_visits_locations_enc, sub { }
    );

    my %user_visit_orig
        = ( user => $user_orig->{hash}, visit => $decoded_orig );
    my $user_visit_orig_enc = $JSON_SORT->encode( \%user_visit_orig );

    $REDIS->zrem( 'val_locations_users_visits_' . $decoded_orig->{location},
      $user_visit_orig_enc, sub { } );

    my %user_visit = ( user => $user->{hash}, visit => $decoded );
    my $user_visit_enc = $JSON_SORT->encode( \%user_visit );

    $REDIS->zadd(
      'val_locations_users_visits_' . $decoded->{location},
      $decoded->{visited_at},
      $user_visit_enc, sub { }
    );
  }

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

  return -1 unless $REDIS->exists( 'val_users_' . $id );

  my $from = $params{fromDate} // 0;
  my $to   = $params{toDate}   // 2147483647;

  my $vals = $REDIS->zrangebyscore( 'val_users_visits_locations_' . $id,
    "($from", "($to" );
  return -1 unless $vals;

  my @res;

  foreach my $val (@$vals) {
    my $decoded = $JSON->decode($val);

    next
        if defined $params{country}
        && $decoded->{location}{country} ne $params{country};
    next
        if defined $params{toDistance}
        && $decoded->{location}{distance} >= $params{toDistance};

    my %visit = (
      mark       => $decoded->{visit}{mark},
      visited_at => $decoded->{visit}{visited_at},
      place      => $decoded->{location}->{place},
    );
    push @res, \%visit;
  }

  return \@res;
}

sub avg {
  my $self   = shift;
  my $id     = shift;
  my %params = @_;

  foreach my $key ( keys %params ) {
    next unless $VALIDATION{$key};
    return -2 if _validate( 'avg', $key, $params{$key} ) == -2;
  }

  return -1 unless $REDIS->exists( 'val_locations_' . $id );

  my $from = $params{fromDate} // 0;
  my $to   = $params{toDate}   // 2147483647;

  my $vals = $REDIS->zrangebyscore( 'val_locations_users_visits_' . $id,
    "($from", "($to" );
  return -1 unless $vals;

  my ( $sum, $cnt ) = ( 0, 0 );

  foreach my $val (@$vals) {
    my $decoded = $JSON->decode($val);

    next if $params{gender} && $decoded->{user}{gender} ne $params{gender};

    if ( $params{fromAge} || $params{toAge} ) {
      my $dt1 = DateTime->from_epoch( epoch => $decoded->{user}{birth_date} );
      my $dt2 = DateTime->today( time_zone => 'UTC' );
      my $dur = $dt2->subtract_datetime($dt1);

      my $age = $dur->years;

      next if $params{fromAge} && $age <= $params{fromAge};
      next if $params{toAge}   && $age >= $params{toAge};
    }

    $cnt++;
    $sum += $decoded->{visit}{mark};
  }

  return 0 unless $cnt;

  my $avg = sprintf( '%.5f', ( $sum / $cnt ) ) + 0;
  return $avg;
}

1;
