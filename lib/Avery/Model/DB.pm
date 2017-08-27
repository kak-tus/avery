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
use List::MoreUtils qw( lower_bound bsearchidx );
use Tie::Array::PackedC Birth    => 'l';
use Tie::Array::PackedC City     => 'S';
use Tie::Array::PackedC Country  => 'C';
use Tie::Array::PackedC Distance => 'S';
use Tie::Array::PackedC FName    => 'C';
use Tie::Array::PackedC Gender   => 'C';
use Tie::Array::PackedC LName    => 'S';
use Tie::Array::PackedC Location => 'L';
use Tie::Array::PackedC Mark     => 'C';
use Tie::Array::PackedC Place    => 'C';
use Tie::Array::PackedC qw(packed_array);
use Tie::Array::PackedC User     => 'L';
use Tie::Array::PackedC Visited  => 'l';
use Time::HiRes qw( gettimeofday tv_interval );

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

my $maps = {
  country    => { idx => 0, map => {} },
  city       => { idx => 0, map => {} },
  place      => { idx => 0, map => {} },
  gender     => { idx => 0, map => {} },
  first_name => { idx => 0, map => {} },
  last_name  => { idx => 0, map => {} },
};

$DAT->{users}{gender}     = Tie::Array::PackedC::Gender::packed_array(1);
$DAT->{users}{first_name} = Tie::Array::PackedC::FName::packed_array(1);
$DAT->{users}{last_name}  = Tie::Array::PackedC::LName::packed_array(1);
$DAT->{users}{birth_date} = Tie::Array::PackedC::Birth::packed_array(1);

$DAT->{locations}{country}  = Tie::Array::PackedC::Country::packed_array(1);
$DAT->{locations}{distance} = Tie::Array::PackedC::Distance::packed_array(1);
$DAT->{locations}{city}     = Tie::Array::PackedC::City::packed_array(1);
$DAT->{locations}{place}    = Tie::Array::PackedC::Place::packed_array(1);

$DAT->{visits}{user}       = Tie::Array::PackedC::User::packed_array(1);
$DAT->{visits}{location}   = Tie::Array::PackedC::Location::packed_array(1);
$DAT->{visits}{visited_at} = Tie::Array::PackedC::Visited::packed_array(1);
$DAT->{visits}{mark}       = Tie::Array::PackedC::Mark::packed_array(1);

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
    if ( defined $maps->{$_}{idx} ) {
      my $idx = $maps->{$_}{map}{ $val->{$_} };
      unless ( defined $idx ) {
        $idx                           = $maps->{$_}{idx};
        $maps->{$_}{map}{ $val->{$_} } = $idx;
        $maps->{$_}{revmap}[$idx]      = $val->{$_};
        $maps->{$_}{idx}++;
      }
      $val->{$_} = $idx;
    }

    $DAT->{$entity}{$_}[ $val->{id} ] = $val->{$_};
  }

  if ( $entity eq 'visits' ) {
    my @list = unpack 'L*', ( $DAT->{_user}[ $val->{user} ] // '' );

    my $idx = lower_bound { $_ <=> $val->{id} } @list;

    if ( $idx < 0 ) {
      push @list, $val->{id};
    }
    else {
      splice @list, $idx, 0, $val->{id};
    }

    $DAT->{_user}[ $val->{user} ] = pack 'L*', @list;

    @list = unpack 'L*', ( $DAT->{_location}[ $val->{location} ] // '' );

    $idx = lower_bound { $_ <=> $val->{id} } @list;

    if ( $idx < 0 ) {
      push @list, $val->{id};
    }
    else {
      splice @list, $idx, 0, $val->{id};
    }

    $DAT->{_location}[ $val->{location} ] = pack 'L*', @list;
  }

  return 1;
}

sub read {
  my $self = shift;
  my ( $entity, $id ) = @_;

  return if $id !~ /^\d+$/;
  return unless defined $DAT->{$entity}{ $entities_fields{$entity}[0] }[$id];

  return '{' . qq{"id":$id,} . join(
    ',',
    map {
            '"'
          . $_ . '":'
          . ( $ints{$_} ? '' : '"' )
          . (
        defined $maps->{$_}{idx}
        ? $maps->{$_}{revmap}[ $DAT->{$entity}{$_}[$id] ]
        : $DAT->{$entity}{$_}[$id]
          )
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

  return -1
      unless defined $DAT->{$entity}{ $entities_fields{$entity}[0] }[$id];

  if ( $entity eq 'visits'
    && $val->{user}
    && $val->{user} != $DAT->{visits}{user}[$id] )
  {
    my @list = unpack 'L*',
        ( $DAT->{_user}[ $DAT->{visits}{user}[$id] ] // '' );

    my $idx = bsearchidx { $_ <=> $id } @list;
    splice @list, $idx, 1;
    $DAT->{_user}[ $DAT->{visits}{user}[$id] ] = pack 'L*', @list;

    @list = unpack 'L*', ( $DAT->{_user}[ $val->{user} ] // '' );

    $idx = lower_bound { $_ <=> $id } @list;

    if ( $idx < 0 ) {
      push @list, $id;
    }
    else {
      splice @list, $idx, 0, $id;
    }

    $DAT->{_user}[ $val->{user} ] = pack 'L*', @list;
  }

  if ( $entity eq 'visits'
    && $val->{location}
    && $val->{location} != $DAT->{visits}{location}[$id] )
  {
    my @list = unpack 'L*',
        ( $DAT->{_location}[ $DAT->{visits}{location}[$id] ] // '' );

    my $idx = bsearchidx { $_ <=> $id } @list;
    splice @list, $idx, 1;
    $DAT->{_location}[ $DAT->{visits}{location}[$id] ] = pack 'L*', @list;

    @list = unpack 'L*', ( $DAT->{_location}[ $val->{location} ] // '' );
    $idx = lower_bound { $_ <=> $id } @list;

    if ( $idx < 0 ) {
      push @list, $id;
    }
    else {
      splice @list, $idx, 0, $id;
    }

    $DAT->{_location}[ $val->{location} ] = pack 'L*', @list;
  }

  foreach ( keys %$val ) {
    if ( defined $maps->{$_}{idx} ) {
      my $idx = $maps->{$_}{map}{ $val->{$_} };
      unless ( defined $idx ) {
        $idx                           = $maps->{$_}{idx};
        $maps->{$_}{map}{ $val->{$_} } = $idx;
        $maps->{$_}{revmap}[$idx]      = $val->{$_};
        $maps->{$_}{idx}++;
      }
      $val->{$_} = $idx;
    }

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

  return -1 unless defined $DAT->{users}{gender}[$id];

  @res = ();

  my $fd = $params->{fromDate};
  my $td = $params->{toDate};
  my $cn = $params->{country};
  my $ds = $params->{toDistance};

  foreach ( unpack 'L*', ( $DAT->{_user}[$id] // '' ) ) {
    next if $fd && $DAT->{visits}{visited_at}[$_] <= $fd;
    next if $td && $DAT->{visits}{visited_at}[$_] >= $td;

    next
        if $cn
        && $maps->{country}{revmap}
        [ $DAT->{locations}{country}[ $DAT->{visits}{location}[$_] ] ] ne $cn;
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
          . $maps->{place}{revmap}
          [ $DAT->{locations}{place}[ $DAT->{visits}{location}[$_] ] ] . '"}',
    );
    push @res, \%visit;
  }

  @sorted
      = map { $_->{enc} } sort { $a->{visited_at} <=> $b->{visited_at} } @res;

  return '{"visits":[' . join( ',', @sorted ) . ']}';
}

sub avg {
  my ( $self, $id, $params ) = @_;

  foreach ( keys %$params ) {
    next unless $VALIDATION{$_};
    return -2 if _validate( 'avg', $_, $params->{$_} ) == -2;
  }

  return -1 unless defined $DAT->{locations}{country}[$id];

  ( $sum, $cnt ) = ( 0, 0 );

  my $fd = $params->{fromDate};
  my $td = $params->{toDate};
  my $fa = $params->{fromAge};
  my $ta = $params->{toAge};
  my $gn = $params->{gender};

  foreach ( unpack 'L*', ( $DAT->{_location}[$id] // '' ) ) {
    next if $fd && $DAT->{visits}{visited_at}[$_] <= $fd;
    next if $td && $DAT->{visits}{visited_at}[$_] >= $td;

    next
        if $gn
        && $maps->{gender}{revmap}
        [ $DAT->{users}{gender}[ $DAT->{visits}{user}[$_] ] ] ne $gn;

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

  my $dt = DateTime->from_epoch( epoch => $birth_date, time_zone => $TZ );
  $DAT->{_years}{$birth_date}
      = $TODAY->clone->subtract_datetime($dt)->years();

  return $DAT->{_years}{$birth_date};
}

1;
