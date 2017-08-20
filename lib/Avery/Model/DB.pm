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
use Time::HiRes;
use List::MoreUtils qw( firstidx bsearchidx );
use Encode qw(decode_utf8);

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
  place      => { len => 2147483647 },
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

  $val->{encoded} = $JSON->encode($val);
  $DAT->{$entity}{ $val->{id} } = $val;

  if ( $entity eq 'visits' ) {
    my $cpos = firstidx { $_->{visit}{visited_at} > $val->{visited_at} }
    @{ $DAT->{_location_visit_by_user}{ $val->{user} } };

    my $pos;
    if ( $cpos < 0 ) {
      $pos = scalar( @{ $DAT->{_location_visit_by_user}{ $val->{user} } } );
      push @{ $DAT->{_location_visit_by_user}{ $val->{user} } }, {};
    }
    else {
      $pos = $cpos;
      splice @{ $DAT->{_location_visit_by_user}{ $val->{user} } },
          $pos, 0, ( {} );
    }

    my $posref = [$pos];

    $DAT->{_location_visit_by_user}{ $val->{user} }[$pos] = {
      location => $DAT->{locations}{ $val->{location} },
      visit    => $val,
      pos      => $posref,
    };

    my $part = int( $val->{visited_at} / 10000000 );
    my $ds   = int( $DAT->{locations}{ $val->{location} }{distance} / 10 );

    _index( 'vs', $val->{user} . '_' . $part, $posref );
    _index( 'cn',
      $val->{user} . '_' . $DAT->{locations}{ $val->{location} }{country},
      $posref );
    _index( 'ds', $val->{user} . '_' . $ds, $posref );
    _index(
      'vscn',
      $val->{user} . '_'
          . $part . '_'
          . $DAT->{locations}{ $val->{location} }{country},
      $posref
    );
    _index( 'vsds', $val->{user} . '_' . $part . '_' . $ds, $posref );
    _index(
      'vscnds',
      $val->{user} . '_'
          . $part . '_'
          . $DAT->{locations}{ $val->{location} }{country} . '_'
          . $ds,
      $posref
    );
    _index(
      'cnds',
      $val->{user} . '_'
          . $DAT->{locations}{ $val->{location} }{country} . '_'
          . $ds,
      $posref
    );

    if ( $cpos >= 0 ) {
      for (
        my $i = $cpos + 1;
        $i < scalar( @{ $DAT->{_location_visit_by_user}{ $val->{user} } } );
        $i++
          )
      {
        $DAT->{_location_visit_by_user}{ $val->{user} }[$i]{pos}[0]++;
      }
    }

    $DAT->{_location_visits}{ $val->{location} }{ $val->{id} } = 1;

    my $years = _years( $DAT->{users}{ $val->{user} }{birth_date} );

    $DAT->{_location_avg}{ $val->{location} }{ $val->{visited_at} }{$years}
        { $DAT->{users}{ $val->{user} }{gender} } ||= [ 0, 0 ];

    $DAT->{_location_avg}{ $val->{location} }{ $val->{visited_at} }{$years}
        { $DAT->{users}{ $val->{user} }{gender} }[0]++;
    $DAT->{_location_avg}{ $val->{location} }{ $val->{visited_at} }{$years}
        { $DAT->{users}{ $val->{user} }{gender} }[1] += $val->{mark};

    $DAT->{_user_avg}{ $val->{user} }{ $val->{location} }
        { $val->{visited_at} } ||= [ 0, 0 ];

    $DAT->{_user_avg}{ $val->{user} }{ $val->{location} }
        { $val->{visited_at} }[0]++;
    $DAT->{_user_avg}{ $val->{user} }{ $val->{location} }
        { $val->{visited_at} }[1] += $val->{mark};
  }

  return 1;
}

sub read {
  my $self = shift;
  my ( $entity, $id ) = @_;

  return unless $DAT->{$entity}{$id};
  return $DAT->{$entity}{$id}{encoded};
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

  delete $new->{encoded};
  $new->{encoded} = $JSON->encode($new);

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

        $DAT->{_location_avg}{$loc}{$at}{$orig_years}{ $orig->{gender} }[0]
            -= $orig_avg->[0];
        $DAT->{_location_avg}{$loc}{$at}{$orig_years}{ $orig->{gender} }[1]
            -= $orig_avg->[1];

        $DAT->{_location_avg}{$loc}{$at}{$years}{ $new->{gender} }
            ||= [ 0, 0 ];

        $DAT->{_location_avg}{$loc}{$at}{$years}{ $new->{gender} }[0]
            += $orig_avg->[0];
        $DAT->{_location_avg}{$loc}{$at}{$years}{ $new->{gender} }[1]
            += $orig_avg->[1];
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
        {$orig_years}{ $DAT->{users}{ $orig->{user} }{gender} }[0]--;
    $DAT->{_location_avg}{ $orig->{location} }{ $orig->{visited_at} }
        {$orig_years}{ $DAT->{users}{ $orig->{user} }{gender} }[1]
        -= $orig->{mark};

    $DAT->{_location_avg}{ $new->{location} }{ $new->{visited_at} }{$years}
        { $DAT->{users}{ $new->{user} }{gender} } ||= [ 0, 0 ];

    $DAT->{_location_avg}{ $new->{location} }{ $new->{visited_at} }{$years}
        { $DAT->{users}{ $new->{user} }{gender} }[0]++;
    $DAT->{_location_avg}{ $new->{location} }{ $new->{visited_at} }{$years}
        { $DAT->{users}{ $new->{user} }{gender} }[1] += $new->{mark};

    $DAT->{_user_avg}{ $orig->{user} }{ $orig->{location} }
        { $orig->{visited_at} }[0]--;
    $DAT->{_user_avg}{ $orig->{user} }{ $orig->{location} }
        { $orig->{visited_at} }[1] -= $orig->{mark};

    $DAT->{_user_avg}{ $new->{user} }{ $new->{location} }
        { $new->{visited_at} } ||= [ 0, 0 ];

    $DAT->{_user_avg}{ $new->{user} }{ $new->{location} }
        { $new->{visited_at} }[0]++;
    $DAT->{_user_avg}{ $new->{user} }{ $new->{location} }
        { $new->{visited_at} }[1] += $new->{mark};
  }

  if (
    $entity eq 'locations'
    && ( $new->{distance} != $orig->{distance}
      || $new->{country} ne $orig->{country} )
      )
  {
    my $orig_ds = int( $orig->{distance} / 10 );
    my $ds      = int( $new->{distance} / 10 );

    foreach my $visid ( keys %{ $DAT->{_location_visits}{$id} } ) {
      my $pos = firstidx { $_->{visit}{id} == $visid }
      @{ $DAT->{_location_visit_by_user}{ $DAT->{visits}{$visid}{user} } };

      my $posref
          = $DAT->{_location_visit_by_user}{ $DAT->{visits}{$visid}{user} }
          [$pos]{pos};
      my $part = int( $DAT->{visits}{$visid}{visited_at} / 10000000 );

      _del_index( 'cn',
        $DAT->{visits}{$visid}{user} . '_' . $orig->{country}, $posref );
      _del_index( 'ds', $DAT->{visits}{$visid}{user} . '_' . $orig_ds,
        $posref );
      _del_index( 'vscn',
        $DAT->{visits}{$visid}{user} . '_' . $part . '_' . $orig->{country},
        $posref );
      _del_index( 'vsds',
        $DAT->{visits}{$visid}{user} . '_' . $part . '_' . $orig_ds,
        $posref );
      _del_index(
        'vscnds',
        $DAT->{visits}{$visid}{user} . '_'
            . $part . '_'
            . $orig->{country} . '_'
            . $orig_ds,
        $posref
      );
      _del_index(
        'cnds',
        $DAT->{visits}{$visid}{user} . '_'
            . $orig->{country} . '_'
            . $orig_ds,
        $posref
      );

      _index( 'cn', $DAT->{visits}{$visid}{user} . '_' . $new->{country},
        $posref );
      _index( 'ds', $DAT->{visits}{$visid}{user} . '_' . $ds, $posref );
      _index( 'vscn',
        $DAT->{visits}{$visid}{user} . '_' . $part . '_' . $new->{country},
        $posref );
      _index( 'vsds', $DAT->{visits}{$visid}{user} . '_' . $part . '_' . $ds,
        $posref );
      _index(
        'vscnds',
        $DAT->{visits}{$visid}{user} . '_'
            . $part . '_'
            . $new->{country} . '_'
            . $ds,
        $posref
      );
      _index( 'cnds',
        $DAT->{visits}{$visid}{user} . '_' . $new->{country} . '_' . $ds,
        $posref );
    }
  }

  if (
    $entity eq 'visits'
    && ( $new->{user} != $orig->{user}
      || $new->{visited_at} != $orig->{visited_at}
      || $new->{location} != $orig->{location} )
      )
  {
    my $orig_part = int( $orig->{visited_at} / 10000000 );

    my $orig_pos = firstidx { $_->{visit}{id} == $id }
    @{ $DAT->{_location_visit_by_user}{ $orig->{user} } };

    splice @{ $DAT->{_location_visit_by_user}{ $orig->{user} } },
        $orig_pos, 1;

    my $orig_posref = [$orig_pos];
    my $orig_ds
        = int( $DAT->{locations}{ $orig->{location} }{distance} / 10 );

    _del_index( 'vs', $orig->{user} . '_' . $orig_part, $orig_posref );

    _del_index( 'cn',
      $orig->{user} . '_' . $DAT->{locations}{ $orig->{location} }{country},
      $orig_posref );
    _del_index( 'ds', $orig->{user} . '_' . $orig_ds, $orig_posref );
    _del_index(
      'vscn',
      $orig->{user} . '_'
          . $orig_part . '_'
          . $DAT->{locations}{ $orig->{location} }{country},
      $orig_posref
    );
    _del_index( 'vsds', $orig->{user} . '_' . $orig_part . '_' . $orig_ds,
      $orig_posref );
    _del_index(
      'vscnds',
      $orig->{user} . '_'
          . $orig_part . '_'
          . $DAT->{locations}{ $orig->{location} }{country} . '_'
          . $orig_ds,
      $orig_posref
    );
    _del_index(
      'cnds',
      $orig->{user} . '_'
          . $DAT->{locations}{ $orig->{location} }{country} . '_'
          . $orig_ds,
      $orig_posref
    );

    for (
      my $i = $orig_pos;
      $i < scalar( @{ $DAT->{_location_visit_by_user}{ $orig->{user} } } );
      $i++
        )
    {
      $DAT->{_location_visit_by_user}{ $orig->{user} }[$i]{pos}[0]--;
    }

    my $cpos = firstidx { $_->{visit}{visited_at} > $new->{visited_at} }
    @{ $DAT->{_location_visit_by_user}{ $new->{user} } };

    my $pos;
    if ( $cpos < 0 ) {
      $pos = scalar( @{ $DAT->{_location_visit_by_user}{ $new->{user} } } );
      push @{ $DAT->{_location_visit_by_user}{ $new->{user} } }, {};
    }
    else {
      $pos = $cpos;
      splice @{ $DAT->{_location_visit_by_user}{ $new->{user} } },
          $pos, 0, ( {} );
    }

    my $part   = int( $new->{visited_at} / 10000000 );
    my $posref = [$pos];
    my $ds
        = int( $DAT->{locations}{ $new->{location} }{distance} / 10 );

    $DAT->{_location_visit_by_user}{ $new->{user} }[$pos] = {
      location => $DAT->{locations}{ $new->{location} },
      visit    => $new,
      pos      => $posref,
    };

    _index( 'vs', $new->{user} . '_' . $part, $posref );
    _index( 'cn',
      $new->{user} . '_' . $DAT->{locations}{ $new->{location} }{country},
      $posref );
    _index( 'ds', $new->{user} . '_' . $ds, $posref );
    _index(
      'vscn',
      $new->{user} . '_'
          . $part . '_'
          . $DAT->{locations}{ $new->{location} }{country},
      $posref
    );
    _index( 'vsds', $new->{user} . '_' . $part . '_' . $ds, $posref );
    _index(
      'vscnds',
      $new->{user} . '_'
          . $part . '_'
          . $DAT->{locations}{ $new->{location} }{country} . '_'
          . $ds,
      $posref
    );
    _index(
      'cnds',
      $new->{user} . '_'
          . $DAT->{locations}{ $new->{location} }{country} . '_'
          . $ds,
      $posref
    );

    if ( $cpos >= 0 ) {
      for (
        my $i = $cpos + 1;
        $i < scalar( @{ $DAT->{_location_visit_by_user}{ $new->{user} } } );
        $i++
          )
      {
        $DAT->{_location_visit_by_user}{ $new->{user} }[$i]{pos}[0]++;
      }
    }

    delete $DAT->{_location_visits}{ $orig->{location} }{$id};
    $DAT->{_location_visits}{ $new->{location} }{$id} = 1;
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

  foreach my $key ( keys %$params ) {
    next unless $VALIDATION{$key};
    return -2 if _validate( 'users_visits', $key, $params->{$key} ) == -2;
  }

  return -1 unless $DAT->{users}{$id};

  my $keys;

  if ( ( $params->{fromDate} || $params->{toDate} )
    && !$params->{country}
    && !$params->{toDistance} )
  {
    my $part1 = int( ( $params->{fromDate} || 0 ) / 10000000 );
    my $part2 = int( ( $params->{toDate}   || 1600000000 ) / 10000000 );
    for ( $part1 .. $part2 ) {
      my $k = _get_index( 'vs', $id . '_' . $_ );
      next unless $k && scalar @$k;
      push @$keys, @$k;
    }
  }
  elsif ( $params->{country}
    && !$params->{fromDate}
    && !$params->{toDate}
    && !$params->{toDistance} )
  {
    $keys = _get_index( 'cn', $id . '_' . $params->{country} );
  }
  elsif ( $params->{toDistance}
    && !$params->{fromDate}
    && !$params->{toDate}
    && !$params->{country} )
  {
    my @t;
    my $ds = int( $params->{toDistance} / 10 );
    for ( 0 .. $ds ) {
      my $k = _get_index( 'ds', $id . '_' . $_ );
      next unless $k && scalar @$k;
      push @t, @$k;
    }

    my @t2 = sort { $a->[0] <=> $b->[0] } @t;
    $keys = \@t2;
  }
  elsif ( ( $params->{fromDate} || $params->{toDate} )
    && $params->{country}
    && !$params->{toDistance} )
  {
    my $part1 = int( ( $params->{fromDate} || 0 ) / 10000000 );
    my $part2 = int( ( $params->{toDate}   || 1600000000 ) / 10000000 );
    for ( $part1 .. $part2 ) {
      my $k = _get_index( 'vscn', $id . '_' . $_ . '_' . $params->{country} );
      next unless $k && scalar @$k;
      push @$keys, @$k;
    }
  }
  elsif ( ( $params->{fromDate} || $params->{toDate} )
    && $params->{toDistance}
    && !$params->{country} )
  {
    my @t;
    my $part1 = int( ( $params->{fromDate} || 0 ) / 10000000 );
    my $part2 = int( ( $params->{toDate}   || 1600000000 ) / 10000000 );
    my $ds = int( $params->{toDistance} / 10 );
    for my $part ( $part1 .. $part2 ) {
      for ( 0 .. $ds ) {
        my $k = _get_index( 'vsds', $id . '_' . $part . '_' . $_ );
        next unless $k && scalar @$k;
        push @t, @$k;
      }
    }

    my @t2 = sort { $a->[0] <=> $b->[0] } @t;
    $keys = \@t2;
  }
  elsif ( ( $params->{fromDate} || $params->{toDate} )
    && $params->{toDistance}
    && $params->{country} )
  {
    my @t;
    my $part1 = int( ( $params->{fromDate} || 0 ) / 10000000 );
    my $part2 = int( ( $params->{toDate}   || 1600000000 ) / 10000000 );
    my $ds = int( $params->{toDistance} / 10 );
    for my $part ( $part1 .. $part2 ) {
      for ( 0 .. $ds ) {
        my $k = _get_index( 'vscnds',
          $id . '_' . $part . '_' . $params->{country} . '_' . $_ );
        next unless $k && scalar @$k;
        push @t, @$k;
      }
    }

    my @t2 = sort { $a->[0] <=> $b->[0] } @t;
    $keys = \@t2;
  }
  else {
    my $part1 = 0;
    my $part2 = int( 1600000000 / 10000000 );
    for ( $part1 .. $part2 ) {
      my $k = _get_index( 'vs', $id . '_' . $_ );
      next unless $k && scalar @$k;
      push @$keys, @$k;
    }
  }

  my @res;

  foreach my $i (@$keys) {
    my $val = $DAT->{_location_visit_by_user}{$id}[ $i->[0] ];

    next
        if $params->{fromDate}
        && $val->{visit}{visited_at} <= $params->{fromDate};
    next
        if $params->{toDate}
        && $val->{visit}{visited_at} >= $params->{toDate};
    next
        if defined $params->{country}
        && $val->{location}{country} ne $params->{country};
    next
        if defined $params->{toDistance}
        && $val->{location}{distance} >= $params->{toDistance};

    my %visit = (
      mark       => $val->{visit}{mark},
      visited_at => $val->{visit}{visited_at},
      place      => $val->{location}{place},
    );
    push @res, \%visit;
  }

  return \@res;
}

sub avg {
  my ( $self, $id, $params ) = @_;

  foreach my $key ( keys %$params ) {
    next unless $VALIDATION{$key};
    return -2 if _validate( 'avg', $key, $params->{$key} ) == -2;
  }

  return -1 unless $DAT->{locations}{$id};

  my ( $sum, $cnt ) = ( 0, 0 );

  my @keys;
  if ( $params->{fromDate} || $params->{toDate} ) {
    $params->{fromDate} //= 0;
    $params->{toDate}   //= 2147483647;
    @keys = grep { $_ > $params->{fromDate} && $_ < $params->{toDate} }
        keys %{ $DAT->{_location_avg}{$id} };
  }
  else {
    @keys = keys %{ $DAT->{_location_avg}{$id} };
  }

  my @genders = qw( m f );
  @genders = ( $params->{gender} ) if $params->{gender};

  foreach my $key (@keys) {
    foreach my $age ( keys %{ $DAT->{_location_avg}{$id}{$key} } ) {
      {
        next
            if $params->{fromAge} && $age < $params->{fromAge};
        next
            if $params->{toAge} && $age >= $params->{toAge};

        foreach my $gender (@genders) {
          next unless $DAT->{_location_avg}{$id}{$key}{$age}{$gender}[0];
          $cnt += $DAT->{_location_avg}{$id}{$key}{$age}{$gender}[0];
          $sum += $DAT->{_location_avg}{$id}{$key}{$age}{$gender}[1];
        }
      }
    }
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

sub _index {
  my ( $name, $val, $pos ) = @_;

  my $cpos
      = bsearchidx { $_->[0] <=> $pos->[0] }
  @{ $DAT->{"_idx_${name}_$val"} };

  if ( $cpos < 0 ) {
    $cpos
        = firstidx { $_->[0] > $pos->[0] } @{ $DAT->{"_idx_${name}_$val"} };
  }

  if ( $cpos < 0 ) {
    push @{ $DAT->{"_idx_${name}_$val"} }, $pos;
  }
  else {
    splice @{ $DAT->{"_idx_${name}_$val"} }, $cpos, 0, ($pos);
  }

  return;
}

sub _get_index {
  my ( $name, $val ) = @_;

  return $DAT->{"_idx_${name}_$val"};
}

sub _del_index {
  my ( $name, $val, $pos ) = @_;

  my $cpos
      = bsearchidx { $_->[0] <=> $pos->[0] }
  @{ $DAT->{"_idx_${name}_$val"} };

  if ( $cpos >= 0 ) {
    splice @{ $DAT->{"_idx_${name}_$val"} }, $cpos, 1;
  }
  else {
    warn 'Del index failed ';
    return -1;
  }

  return;
}

1;
