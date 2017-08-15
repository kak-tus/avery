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
use Encode qw(encode_utf8);
use List::Util qw(any);
use Time::HiRes;

my $DAT;

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
  $self->{logger}->info("Start $start");

  foreach my $file (@files) {
    open my $fl, "$file";
    my $st = <$fl>;
    close $fl;

    my $decoded = $JSON->decode($st);

    my $entity = ( keys %$decoded )[0];

    foreach my $val ( @{ $decoded->{$entity} } ) {
      if ( 0 && $entity eq 'visits' ) {
        for ( 1 .. 100 ) {
          $val->{id} += 100000;
          $self->create( $entity, $val, without_validation => 1 );
        }
      }
      else {
        $self->create( $entity, $val, without_validation => 1 );
      }
    }
  }

  my $end = Time::HiRes::time;
  $self->{logger}->info( "Loaded $end, diff " . ( $end - $start ) );

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

  $DAT->{$entity}{ $val->{id} } = $val;

  if ( $entity eq 'visits' ) {
    $DAT->{_location_visit_by_user}{ $val->{user} }{ $val->{visited_at} }
        { $val->{id} } = {
      location => $DAT->{locations}{ $val->{location} },
      visit    => $val,
        };

    $DAT->{_user_visit_by_location}{ $val->{location} }{ $val->{visited_at} }
        { $val->{id} }
        = { user => $DAT->{users}{ $val->{user} }, visit => $val };
  }

  return 1;
}

sub read {
  my $self = shift;
  my ( $entity, $id ) = @_;

  return $DAT->{$entity}{$id};
}

sub update {
  my $self = shift;
  my ( $entity, $id, $val ) = @_;

  my $orig = $DAT->{$entity}{$id};
  return -1 unless $orig;

  foreach my $key ( keys %$val ) {
    if ( $VALIDATION{$key} ) {
      return -2 if _validate( 'update', $key, $val->{$key} ) == -2;
    }
  }

  if (
    $entity eq 'visits'
    && (
      ( $val->{user} && $val->{user} != $orig->{user} )
      || ( $val->{visited_at}
        && $val->{visited_at} != $orig->{visited_at} )
    )
      )
  {
    delete $DAT->{_location_visit_by_user}{ $orig->{user} }
        { $orig->{visited_at} }{$id};

    $DAT->{_location_visit_by_user}{ $val->{user}
          || $orig->{user} }{ $val->{visited_at}
          || $orig->{visited_at} }{$id} = {
      location => $DAT->{locations}{ $val->{location} || $orig->{location} },
      visit => $orig,
          };
  }
  if (
    $entity eq 'visits'
    && ( ( $val->{location} && $val->{location} != $orig->{location} )
      || ( $val->{visited_at} && $val->{visited_at} != $orig->{visited_at} ) )
      )
  {
    delete $DAT->{_user_visit_by_location}{ $orig->{location} }
        { $orig->{visited_at} }{$id};

    $DAT->{_user_visit_by_location}{ $val->{location}
          || $orig->{location} }{ $val->{visited_at}
          || $orig->{visited_at} }{$id} = {
      user => $DAT->{users}{ $val->{user} || $orig->{user} },
      visit => $orig,
          };
  }

  foreach my $key ( keys %$val ) {
    $orig->{$key} = $val->{$key};
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
  my $self   = shift;
  my $id     = shift;
  my %params = @_;

  foreach my $key ( keys %params ) {
    next unless $VALIDATION{$key};
    return -2 if _validate( 'users_visits', $key, $params{$key} ) == -2;
  }

  return -1 unless $DAT->{users}{$id};

  my @keys;
  if ( $params{fromDate} || $params{toDate} ) {
    $params{fromDate} //= 0;
    $params{toDate}   //= 2147483647;
    @keys = grep { $_ >= $params{fromDate} && $_ <= $params{toDate} }
        keys %{ $DAT->{_location_visit_by_user}{$id} };
  }
  else {
    @keys = keys %{ $DAT->{_location_visit_by_user}{$id} };
  }

  my @res;

  foreach my $key (@keys) {
    foreach my $val ( values %{ $DAT->{_location_visit_by_user}{$id}{$key} } )
    {
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

  my @keys;
  if ( $params{fromDate} || $params{toDate} ) {
    $params{fromDate} //= 0;
    $params{toDate}   //= 2147483647;
    @keys = grep { $_ >= $params{fromDate} && $_ <= $params{toDate} }
        keys %{ $DAT->{_user_visit_by_location}{$id} };
  }
  else {
    @keys = keys %{ $DAT->{_user_visit_by_location}{$id} };
  }

  foreach my $key (@keys) {
    foreach my $val ( values %{ $DAT->{_user_visit_by_location}{$id}{$key} } )
    {
      next if $params{gender} && $params{gender} ne $val->{user}{gender};
      next if $dt_from        && $dt_from < $val->{user}{birth_date};
      next if $dt_to          && $dt_to >= $val->{user}{birth_date};

      $cnt++;
      $sum += $val->{visit}{mark};
    }
  }

  return 0 unless $cnt;

  my $avg = sprintf( '%.5f', ( $sum / $cnt ) ) + 0;

  return $avg;
}

1;
