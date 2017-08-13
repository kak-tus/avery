package Avery::Model::DB;

use strict;
use warnings;
use v5.10;
use utf8;

use AnyEvent;
use Clone qw(clone);
use Cpanel::JSON::XS;
use Data::Dumper;
use DateTime;
use DateTime::TimeZone;
use Encode qw(encode_utf8);
use IPC::ShareLite;
use List::Util qw(any);
use Sereal qw(sereal_encode_with_object sereal_decode_with_object);
use Storable qw( freeze thaw );
use Time::HiRes;

my $enc = Sereal::Encoder->new();
my $dec = Sereal::Decoder->new();

my $DAT;

my $JSON = Cpanel::JSON::XS->new->utf8;

my $TZ = DateTime::TimeZone->new( name => 'UTC' );
my $TODAY = DateTime->today( time_zone => $TZ );

my $SHARE = IPC::ShareLite->new(
  -key     => 2,
  -create  => 'yes',
  -destroy => 'yes',
) or die $!;

my $UPDATE_TIME;
my $SHARE_TIME = IPC::ShareLite->new(
  -key     => 3,
  -create  => 'yes',
  -destroy => 'yes',
) or die $!;

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

my $TIMER;

sub new {
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
      my $status = $self->create(
        $entity, $val,
        without_validation => 1,
        without_store      => 1,
      );
    }
  }

  _update_share();

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

  if ( !$params{without_store} ) {
    _read_share();
  }

  $DAT->{$entity}{ $val->{id} } = $val;

  if ( $entity eq 'visits' ) {
    $DAT->{_location_visit_by_user}{ $val->{user} }{ $val->{id} } = {
      location => $DAT->{locations}{ $val->{location} },
      visit    => $val
    };
    $DAT->{_user_visit_by_location}{ $val->{location} }{ $val->{id} }
        = { user => $DAT->{users}{ $val->{user} }, visit => $val };
  }

  if ( !$params{without_store} ) {
    _update_share();
  }

  return 1;
}

sub read {
  my $self = shift;
  my ( $entity, $id ) = @_;

  _read_share();
  return $DAT->{$entity}{$id};
}

sub update {
  my $self = shift;
  my ( $entity, $id, $val ) = @_;

  _read_share();

  my $orig = $DAT->{$entity}{$id};
  return -1 unless $orig;

  foreach my $key ( keys %$val ) {
    if ( $VALIDATION{$key} ) {
      return -2 if _validate( 'update', $key, $val->{$key} ) == -2;
    }
  }

  if ( $entity eq 'visits'
    && $val->{user}
    && $val->{user} != $orig->{user} )
  {
    delete $DAT->{_location_visit_by_user}{ $orig->{user} }{$id};
    $DAT->{_location_visit_by_user}{ $val->{user} }{$id} = {
      location => $DAT->{locations}{ $val->{location} || $orig->{location} },
      visit => $orig,
    };
  }
  if ( $entity eq 'visits'
    && $val->{location}
    && $val->{location} != $orig->{location} )
  {
    delete $DAT->{_user_visit_by_location}{ $orig->{location} }{$id};
    $DAT->{_user_visit_by_location}{ $val->{location} }{$id} = {
      user => $DAT->{users}{ $val->{user} || $orig->{user} },
      visit => $orig,
    };
  }

  foreach my $key ( keys %$val ) {
    $orig->{$key} = $val->{$key};
  }

  _update_share();

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
  my $self   = shift;
  my $id     = shift;
  my %params = @_;

  foreach my $key ( keys %params ) {
    next unless $VALIDATION{$key};
    return -2 if _validate( 'users_visits', $key, $params{$key} ) == -2;
  }

  _read_share();

  return -1 unless $DAT->{users}{$id};

  my @res;

  foreach my $val ( values %{ $DAT->{_location_visit_by_user}{$id} } ) {
    next
        if $params{fromDate}
        && $val->{visit}{visited_at} < $params{fromDate};
    next if $params{toDate} && $val->{visit}{visited_at} > $params{toDate};

    next
        if defined $params{country}
        && $val->{location}{country} ne $params{country};
    next
        if defined $params{toDistance}
        && $val->{location}{distance} >= $params{toDistance};

    my %visit = (
      mark       => $val->{visit}{mark},
      visited_at => $val->{visit}{visited_at},
      place      => $val->{location}{place},
    );
    push @res, \%visit;
  }

  my @sorted = sort { $a->{visited_at} <=> $b->{visited_at} } @res;
  return \@sorted;
}

sub avg {
  my $self   = shift;
  my $id     = shift;
  my %params = @_;

  foreach my $key ( keys %params ) {
    next unless $VALIDATION{$key};
    return -2 if _validate( 'avg', $key, $params{$key} ) == -2;
  }

  _read_share();

  return -1 unless $DAT->{locations}{$id};

  my ( $sum, $cnt ) = ( 0, 0 );

  my ( $dt_from, $dt_to );

  if ( $params{fromAge} ) {
    $dt_from
        = $TODAY->clone->subtract( years => $params{fromAge} )->epoch();
  }
  if ( $params{toAge} ) {
    $dt_to = $TODAY->clone->subtract( years => $params{toAge} )->epoch();
  }

  foreach my $val ( values %{ $DAT->{_user_visit_by_location}{$id} } ) {
    next
        if $params{fromDate}
        && $val->{visit}{visited_at} < $params{fromDate};
    next if $params{toDate} && $val->{visit}{visited_at} > $params{toDate};

    next if $params{gender} && $val->{user}{gender} ne $params{gender};

    next if $dt_from && $dt_from < $val->{user}{birth_date};
    next if $dt_to   && $dt_to >= $val->{user}{birth_date};

    $cnt++;
    $sum += $val->{visit}{mark};
  }

  return 0 unless $cnt;

  my $avg = sprintf( '%.5f', ( $sum / $cnt ) ) + 0;
  return $avg;
}

sub _update_share {
  $SHARE->store( sereal_encode_with_object( $enc, $DAT ) );
  $UPDATE_TIME = Time::HiRes::time;
  $SHARE_TIME->store($UPDATE_TIME);

  return;
}

sub _read_share {
  my $upd = $SHARE_TIME->fetch;
  return if $UPDATE_TIME && $upd eq $UPDATE_TIME;

  $DAT = sereal_decode_with_object( $dec, $SHARE->fetch );
  $UPDATE_TIME = $upd;

  return;
}

1;
