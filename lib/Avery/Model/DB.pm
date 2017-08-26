package Avery::Model::DB;

use strict;
use warnings;
use v5.10;
use utf8;

use Clone qw(clone);
use Cpanel::JSON::XS;
use Data::Dumper;
use DateTime;
use DateTime::TimeZone;
use Time::HiRes qw( gettimeofday tv_interval );
use List::MoreUtils qw( lower_bound bsearchidx );

our $DAT;

my $JSON = Cpanel::JSON::XS->new->utf8;

my $TZ = DateTime::TimeZone->new( name => 'UTC' );
my $TODAY = DateTime->today( time_zone => $TZ );

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
  place      => { len => 2147483647 },
);

my $t0;
my @res;
my ( $sum, $cnt );
my @keys;
my @sorted;

my %entities_fields = (
  users => [
    qw(
        gender
        first_name
        last_name
        birth_date
        email
        )
  ],
  locations => [
    qw(
        country
        distance
        city
        place
        )
  ],
  visits => [
    qw(
        user
        location
        visited_at
        mark
        )
  ],
);

my %ints = (
  id         => 1,
  birth_date => 1,
  user       => 1,
  location   => 1,
  visited_at => 1,
  mark       => 1,
  distance   => 1,
);

sub new {
  my $parent = shift;
  my %params = @_;

  return
      bless { parent_pid => $params{parent_pid}, logger => $params{logger} };
}

sub load {
  my $self = shift;

  my @files = glob '/tmp/unzip/users*.json';
  push @files, glob '/tmp/unzip/locations*.json';
  push @files, glob '/tmp/unzip/visits*.json';

  my $start = Time::HiRes::time;
  $self->{logger}->INFO("Start $start");

  foreach my $file (@files) {
    open my $fl, "$file";
    my $st = <$fl>;
    close $fl;

    my $decoded = $JSON->decode($st);

    my $entity = ( keys %$decoded )[0];

    foreach my $val ( @{ $decoded->{$entity} } ) {
      $self->create( $entity, $val, without_validation => 1 );
    }
  }

  my $end = Time::HiRes::time;
  $self->{logger}->INFO( "Loaded $end, diff " . ( $end - $start ) );

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

  foreach ( @{ $entities_fields{$entity} } ) {
    $DAT->{$entity}{$_}[ $val->{id} ] = $val->{$_};
  }

  if ( $entity eq 'visits' ) {
    my $idx = lower_bound { $_ <=> $val->{id} }
    @{ $DAT->{_user}{ $val->{user} } };

    if ( $idx < 0 ) {
      push @{ $DAT->{_user}{ $val->{user} } }, $val->{id};
    }
    else {
      splice @{ $DAT->{_user}{ $val->{user} } }, $idx, 0, $val->{id};
    }

    $idx = lower_bound { $_ <=> $val->{id} }
    @{ $DAT->{_location}{ $val->{location} } };

    if ( $idx < 0 ) {
      push @{ $DAT->{_location}{ $val->{location} } }, $val->{id};
    }
    else {
      splice @{ $DAT->{_location}{ $val->{location} } }, $idx, 0, $val->{id};
    }
  }

  return 1;
}

sub read {
  my $self = shift;
  my ( $entity, $id ) = @_;

  return if $id eq 'bad';
  return unless $DAT->{$entity}{ $entities_fields{$entity}[0] }[$id];

  return '{' . join(
    ',',
    map {
            qq{"id":$id,} . '"'
          . $_ . '":'
          . ( $ints{$_} ? '' : '"' )
          . $DAT->{$entity}{$_}[$id]
          . ( $ints{$_} ? '' : '"' )
    } @{ $entities_fields{$entity} }
  ) . '}';
}

sub update {
  my $self = shift;
  my ( $entity, $id, $val ) = @_;

  foreach ( keys %$val ) {
    if ( $VALIDATION{$_} ) {
      return -2 if _validate( 'update', $_, $val->{$_} ) == -2;
    }
  }

  return -1 unless $DAT->{$entity}{ $entities_fields{$entity}[0] }[$id];

  if ( $entity eq 'visits'
    && $val->{user}
    && $val->{user} != $DAT->{visits}{user}[$id] )
  {
    my $idx = bsearchidx { $_ <=> $id }
    @{ $DAT->{_user}{ $DAT->{visits}{user}[$id] } };

    splice @{ $DAT->{_user}{ $DAT->{visits}{user}[$id] } }, $idx, 1;

    $idx = lower_bound { $_ <=> $id } @{ $DAT->{_user}{ $val->{user} } };

    if ( $idx < 0 ) {
      push @{ $DAT->{_user}{ $val->{user} } }, $id;
    }
    else {
      splice @{ $DAT->{_user}{ $val->{user} } }, $idx, 0, $id;
    }
  }

  if ( $entity eq 'visits'
    && $val->{location}
    && $val->{location} != $DAT->{visits}{location}[$id] )
  {
    my $idx = bsearchidx { $_ <=> $id }
    @{ $DAT->{_location}{ $DAT->{visits}{location}[$id] } };

    splice @{ $DAT->{_location}{ $DAT->{visits}{location}[$id] } }, $idx, 1;

    $idx = lower_bound { $_ <=> $id }
    @{ $DAT->{_location}{ $val->{location} } };

    if ( $idx < 0 ) {
      push @{ $DAT->{_location}{ $val->{location} } }, $id;
    }
    else {
      splice @{ $DAT->{_location}{ $val->{location} } }, $idx, 0, $id;
    }
  }

  foreach ( keys %$val ) {
    $DAT->{$entity}{$_}[$id] = $val->{$_};
  }

  return 1;
}

sub _validate {
  my ( $action, $key, $val ) = @_;

  if ( $VALIDATION{$key}->{len} ) {
    if (!defined($val)
      || length($val) > $VALIDATION{$key}->{len} )
    {
      return -2;
    }
  }
  elsif ( $VALIDATION{$key}->{in} ) {
    if ( !defined($val) || !$VALIDATION{$key}->{in}->{$val} ) {
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
      return -2;
    }
  }

  return 1;
}

sub users_visits {
  my ( $self, $id, $params ) = @_;

  foreach ( keys %$params ) {
    next unless $VALIDATION{$_};
    return -2 if _validate( 'users_visits', $_, $params->{$_} ) == -2;
  }

  return -1 unless $DAT->{users}{gender}[$id];

  @res = ();

  my $fd = $params->{fromDate};
  my $td = $params->{toDate};
  my $cn = $params->{country};
  my $ds = $params->{toDistance};

  # use Cpanel::JSON::XS;
  # my $JSON = Cpanel::JSON::XS->new->utf8;
  # $t0=[gettimeofday];
  # for(1..100){

  foreach ( @{ $DAT->{_user}{$id} } ) {
    next if $fd && $DAT->{visits}{visited_at}[$_] <= $fd;
    next if $td && $DAT->{visits}{visited_at}[$_] >= $td;

    next
        if $cn
        && $DAT->{locations}{country}[ $DAT->{visits}{location}[$_] ] ne $cn;
    next
        if $ds
        && $DAT->{locations}{distance}[ $DAT->{visits}{location}[$_] ] >= $ds;

    my %visit = (
      visited_at => $DAT->{visits}{visited_at}[$_],
      enc        => '{"mark":'
          . $DAT->{visits}{mark}[$_]
          . ',"visited_at":'
          . $DAT->{visits}{visited_at}[$_]
          . ',"place":"'
          . $DAT->{locations}{place}[ $DAT->{visits}{location}[$_] ] . '"}',
    );
    push @res, \%visit;
  }

  @sorted
      = map { $_->{enc} } sort { $a->{visited_at} <=> $b->{visited_at} } @res;

  # }
  # say tv_interval($t0)*1000000;

  return '{"visits":[' . join( ',', @sorted ) . ']}';
}

sub avg {
  my ( $self, $id, $params ) = @_;

  foreach ( keys %$params ) {
    next unless $VALIDATION{$_};
    return -2 if _validate( 'avg', $_, $params->{$_} ) == -2;
  }

  return -1 unless $DAT->{locations}{country}[$id];

  ( $sum, $cnt ) = ( 0, 0 );

  my $fd = $params->{fromDate};
  my $td = $params->{toDate};
  my $fa = $params->{fromAge};
  my $ta = $params->{toAge};
  my $gn = $params->{gender};

  foreach ( @{ $DAT->{_location}{$id} } ) {
    next if $fd && $DAT->{visits}{visited_at}[$_] <= $fd;
    next if $td && $DAT->{visits}{visited_at}[$_] >= $td;

    next
        if $gn
        && $DAT->{users}{gender}[ $DAT->{visits}{user}[$_] ] ne $gn;

    next
        if $fa
        && _years( $DAT->{users}{birth_date}[ $DAT->{visits}{user}[$_] ] )
        < $fa;
    next
        if $ta
        && _years( $DAT->{users}{birth_date}[ $DAT->{visits}{user}[$_] ] )
        >= $ta;

    $cnt++;
    $sum += $DAT->{visits}{mark}[$_];
  }

  return 0 unless $cnt;

  my $avg = sprintf( '%.5f', ( $sum / $cnt + 0.0000001 ) ) + 0;

  return $avg;
}

sub _years {
  my $birth_date = shift;

  return $DAT->{_years}{$birth_date} if $DAT->{_years}{$birth_date};

  my $dt = DateTime->from_epoch( epoch => $birth_date, time_zone => $TZ, );
  $DAT->{_years}{$birth_date}
      = $TODAY->clone->subtract_datetime($dt)->years();

  return $DAT->{_years}{$birth_date};
}

1;
