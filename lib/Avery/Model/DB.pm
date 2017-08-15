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
      $self->create( $entity, $val, without_validation => 1 );
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

    my $years = _years( $DAT->{users}{ $val->{user} }{birth_date} );

    $DAT->{_location_avg}{ $val->{location} }{ $val->{visited_at} }{$years}
        { $DAT->{users}{ $val->{user} }{gender} } ||= { cnt => 0, sum => 0 };

    $DAT->{_location_avg}{ $val->{location} }{ $val->{visited_at} }{$years}
        { $DAT->{users}{ $val->{user} }{gender} }{cnt}++;
    $DAT->{_location_avg}{ $val->{location} }{ $val->{visited_at} }{$years}
        { $DAT->{users}{ $val->{user} }{gender} }{sum} += $val->{mark};

    $DAT->{_user_avg}{ $val->{user} }{ $val->{location} }
        { $val->{visited_at} } ||= { cnt => 0, sum => 0 };

    $DAT->{_user_avg}{ $val->{user} }{ $val->{location} }
        { $val->{visited_at} }{cnt}++;
    $DAT->{_user_avg}{ $val->{user} }{ $val->{location} }
        { $val->{visited_at} }{sum} += $val->{mark};
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

  my $new = $DAT->{$entity}{$id};
  return -1 unless $new;

  foreach my $key ( keys %$val ) {
    if ( $VALIDATION{$key} ) {
      return -2 if _validate( 'update', $key, $val->{$key} ) == -2;
    }
  }

  my $orig = clone($new);

  foreach my $key ( keys %$val ) {
    $new->{$key} = $val->{$key};
  }

  if (
    $entity eq 'users'
    && ( $new->{gender} ne $orig->{gender}
      || $new->{birth_date} ne $orig->{birth_date} )
      )
  {
    my $orig_years = _years( $orig->{birth_date} );
    my $years      = _years( $new->{birth_date} );

    foreach my $loc ( keys %{ $DAT->{_user_avg}{$id} } ) {
      foreach my $at ( keys %{ $DAT->{_user_avg}{$id}{$loc} } ) {
        my $orig_avg = $DAT->{_user_avg}{$id}{$loc}{$at};

        $DAT->{_location_avg}{$loc}{$at}{$orig_years}{ $orig->{gender} }{cnt}
            -= $orig_avg->{cnt};
        $DAT->{_location_avg}{$loc}{$at}{$orig_years}{ $orig->{gender} }{sum}
            -= $orig_avg->{sum};

        $DAT->{_location_avg}{$loc}{$at}{$years}{ $new->{gender} }
            ||= { cnt => 0, sum => 0 };

        $DAT->{_location_avg}{$loc}{$at}{$years}{ $new->{gender} }{cnt}
            += $orig_avg->{cnt};
        $DAT->{_location_avg}{$loc}{$at}{$years}{ $new->{gender} }{sum}
            += $orig_avg->{sum};
      }
    }
  }

  if (
    $entity eq 'visits'
    && ( $new->{location} ne $orig->{location}
      || $new->{visited_at} ne $orig->{visited_at}
      || $new->{user} ne $orig->{user}
      || $new->{mark} ne $orig->{mark} )
      )
  {
    my $orig_years = _years( $DAT->{users}{ $orig->{user} }{birth_date} );
    my $years      = _years( $DAT->{users}{ $new->{user} }{birth_date} );

    $DAT->{_location_avg}{ $orig->{location} }{ $orig->{visited_at} }
        {$orig_years}{ $DAT->{users}{ $orig->{user} }{gender} }{cnt}--;
    $DAT->{_location_avg}{ $orig->{location} }{ $orig->{visited_at} }
        {$orig_years}{ $DAT->{users}{ $orig->{user} }{gender} }{sum}
        -= $orig->{mark};

    $DAT->{_location_avg}{ $new->{location} }{ $new->{visited_at} }{$years}
        { $DAT->{users}{ $new->{user} }{gender} } ||= { cnt => 0, sum => 0 };

    $DAT->{_location_avg}{ $new->{location} }{ $new->{visited_at} }{$years}
        { $DAT->{users}{ $new->{user} }{gender} }{cnt}++;
    $DAT->{_location_avg}{ $new->{location} }{ $new->{visited_at} }{$years}
        { $DAT->{users}{ $new->{user} }{gender} }{sum} += $new->{mark};

    $DAT->{_user_avg}{ $orig->{user} }{ $orig->{location} }
        { $orig->{visited_at} }{cnt}--;
    $DAT->{_user_avg}{ $orig->{user} }{ $orig->{location} }
        { $orig->{visited_at} }{sum} -= $orig->{mark};

    $DAT->{_user_avg}{ $new->{user} }{ $new->{location} }
        { $new->{visited_at} } ||= { cnt => 0, sum => 0 };

    $DAT->{_user_avg}{ $new->{user} }{ $new->{location} }
        { $new->{visited_at} }{cnt}++;
    $DAT->{_user_avg}{ $new->{user} }{ $new->{location} }
        { $new->{visited_at} }{sum} += $new->{mark};
  }

  if (
    $entity eq 'visits'
    && ( ( $new->{user} != $orig->{user} )
      || ( $new->{visited_at} != $orig->{visited_at} ) )
      )
  {
    delete $DAT->{_location_visit_by_user}{ $orig->{user} }
        { $orig->{visited_at} }{$id};

    $DAT->{_location_visit_by_user}{ $new->{user} }{ $new->{visited_at} }{$id}
        = {
      location => $DAT->{locations}{ $new->{location} },
      visit    => $orig,
        };
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

  my @keys;
  if ( $params{fromDate} || $params{toDate} ) {
    $params{fromDate} //= 0;
    $params{toDate}   //= 2147483647;
    @keys = grep { $_ >= $params{fromDate} && $_ <= $params{toDate} }
        keys %{ $DAT->{_location_avg}{$id} };
  }
  else {
    @keys = keys %{ $DAT->{_location_avg}{$id} };
  }

  my @genders = qw( m f );
  @genders = ( $params{gender} ) if $params{gender};

  foreach my $key (@keys) {
    foreach my $age ( keys %{ $DAT->{_location_avg}{$id}{$key} } ) {
      {
        next
            if $params{fromAge} && $age < $params{fromAge};
        next
            if $params{toAge} && $age >= $params{toAge};

        foreach my $gender (@genders) {
          next unless $DAT->{_location_avg}{$id}{$key}{$age}{$gender}{cnt};
          $cnt += $DAT->{_location_avg}{$id}{$key}{$age}{$gender}{cnt};
          $sum += $DAT->{_location_avg}{$id}{$key}{$age}{$gender}{sum};
        }
      }
    }
  }

  return 0 unless $cnt;

  my $avg = sprintf( '%.5f', ( $sum / $cnt ) ) + 0;

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
