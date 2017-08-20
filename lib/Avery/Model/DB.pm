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

    # use Devel::Size qw(total_size);
    # $self->{logger}->INFO( total_size( $DAT->{visits} ) );
    # $self->{logger}->INFO( total_size( $DAT->{_location_visit_by_user} ) );
    # $self->{logger}->INFO( total_size( $DAT->{_location_visits} ) );
    # $self->{logger}->INFO( total_size( $DAT->{_location_avg} ) );
    # $self->{logger}->INFO( total_size( $DAT->{_user_avg} ) );
    # $self->{logger}->INFO($file);

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

  $DAT->{$entity}{ $val->{id} } = $JSON->encode($val);

  if ( $entity eq 'visits' ) {
    my $cpos = firstidx { $_->{v} > $val->{visited_at} }
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

    my $posref = \$pos;
    my $loc    = $JSON->decode( $DAT->{locations}{ $val->{location} } );

    $DAT->{_location_visit_by_user}{ $val->{user} }[$pos] = {
      p => $posref,
      e => $JSON->encode(
        { mark       => $val->{mark},
          visited_at => $val->{visited_at},
          place      => $loc->{place},
        }
      ),
      v => $val->{visited_at},
      c => $loc->{country},
      d => $loc->{distance},
      i => $val->{id},
    };

    my $part = int( $val->{visited_at} / 10000000 );
    my $ds   = int( $loc->{distance} / 10 );

    _index( 'v', $val->{user} . '_' . $part,           $posref );
    _index( 'c', $val->{user} . '_' . $loc->{country}, $posref );
    _index( 'd', $val->{user} . '_' . $ds,             $posref );
    _index( 'vc', $val->{user} . '_' . $part . '_' . $loc->{country},
      $posref );
    _index( 'vd', $val->{user} . '_' . $part . '_' . $ds, $posref );
    _index( 'vcd',
      $val->{user} . '_' . $part . '_' . $loc->{country} . '_' . $ds,
      $posref );
    _index( 'cd', $val->{user} . '_' . $loc->{country} . '_' . $ds, $posref );

    if ( $cpos >= 0 ) {
      for (
        my $i = $cpos + 1;
        $i < scalar( @{ $DAT->{_location_visit_by_user}{ $val->{user} } } );
        $i++
          )
      {
        ${ $DAT->{_location_visit_by_user}{ $val->{user} }[$i]{p} }++;
      }
    }

    $DAT->{_location_visits}{ $val->{location} }{ $val->{id} } = 1;

    my $usr   = $JSON->decode( $DAT->{users}{ $val->{user} } );
    my $years = _years( $usr->{birth_date} );

    $DAT->{_location_avg}{ $val->{location} }{ $val->{visited_at} }{$years}
        { $usr->{gender} } ||= [ 0, 0 ];

    $DAT->{_location_avg}{ $val->{location} }{ $val->{visited_at} }{$years}
        { $usr->{gender} }[0]++;
    $DAT->{_location_avg}{ $val->{location} }{ $val->{visited_at} }{$years}
        { $usr->{gender} }[1] += $val->{mark};

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

  $new = $JSON->decode($new);
  my $orig = clone($new);

  foreach my $key ( keys %$val ) {
    $new->{$key} = $val->{$key};
  }

  $DAT->{$entity}{$id} = $JSON->encode($new);

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
    my $orig_user = $JSON->decode( $DAT->{users}{ $orig->{user} } );
    my $user      = $JSON->decode( $DAT->{users}{ $new->{user} } );

    my $orig_years = _years( $orig_user->{birth_date} );
    my $years      = _years( $user->{birth_date} );

    $DAT->{_location_avg}{ $orig->{location} }{ $orig->{visited_at} }
        {$orig_years}{ $orig_user->{gender} }[0]--;
    $DAT->{_location_avg}{ $orig->{location} }{ $orig->{visited_at} }
        {$orig_years}{ $orig_user->{gender} }[1] -= $orig->{mark};

    $DAT->{_location_avg}{ $new->{location} }{ $new->{visited_at} }
        {$years}{ $user->{gender} } ||= [ 0, 0 ];

    $DAT->{_location_avg}{ $new->{location} }{ $new->{visited_at} }
        {$years}{ $user->{gender} }[0]++;
    $DAT->{_location_avg}{ $new->{location} }{ $new->{visited_at} }
        {$years}{ $user->{gender} }[1] += $new->{mark};

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
      || $new->{country} ne $orig->{country}
      || $new->{place} ne $orig->{place} )
      )
  {
    my $orig_ds = int( $orig->{distance} / 10 );
    my $ds      = int( $new->{distance} / 10 );

    foreach my $visid ( keys %{ $DAT->{_location_visits}{$id} } ) {
      my $visit = $JSON->decode( $DAT->{visits}{$visid} );

      my $pos = firstidx { $_->{i} == $visid }
      @{ $DAT->{_location_visit_by_user}{ $visit->{user} } };

      $DAT->{_location_visit_by_user}{ $visit->{user} }[$pos]{e}
          = $JSON->encode(
        { mark       => $visit->{mark},
          visited_at => $visit->{visited_at},
          place      => $new->{place},
        }
          );

      $DAT->{_location_visit_by_user}{ $visit->{user} }[$pos]{c}
          = $new->{country};
      $DAT->{_location_visit_by_user}{ $visit->{user} }[$pos]{d}
          = $new->{distance};

      my $posref = $DAT->{_location_visit_by_user}{ $visit->{user} }[$pos]{p};
      my $part   = int( $visit->{visited_at} / 10000000 );

      _del_index( 'c', $visit->{user} . '_' . $orig->{country}, $posref );
      _del_index( 'd', $visit->{user} . '_' . $orig_ds,         $posref );
      _del_index( 'vc', $visit->{user} . '_' . $part . '_' . $orig->{country},
        $posref );
      _del_index( 'vd', $visit->{user} . '_' . $part . '_' . $orig_ds,
        $posref );
      _del_index(
        'vcd',
        $visit->{user} . '_'
            . $part . '_'
            . $orig->{country} . '_'
            . $orig_ds,
        $posref
      );
      _del_index( 'cd',
        $visit->{user} . '_' . $orig->{country} . '_' . $orig_ds, $posref );

      _index( 'c', $visit->{user} . '_' . $new->{country}, $posref );
      _index( 'd', $visit->{user} . '_' . $ds,             $posref );
      _index( 'vc', $visit->{user} . '_' . $part . '_' . $new->{country},
        $posref );
      _index( 'vd', $visit->{user} . '_' . $part . '_' . $ds, $posref );
      _index( 'vcd',
        $visit->{user} . '_' . $part . '_' . $new->{country} . '_' . $ds,
        $posref );
      _index( 'cd', $visit->{user} . '_' . $new->{country} . '_' . $ds,
        $posref );
    }
  }

  if (
    $entity eq 'visits'
    && ( $new->{user} != $orig->{user}
      || $new->{visited_at} != $orig->{visited_at}
      || $new->{location} != $orig->{location}
      || $new->{mark} != $orig->{mark} )
      )
  {
    my $orig_loc = $JSON->decode( $DAT->{locations}{ $orig->{location} } );
    my $loc      = $JSON->decode( $DAT->{locations}{ $new->{location} } );

    my $orig_part = int( $orig->{visited_at} / 10000000 );

    my $orig_pos = firstidx { $_->{i} == $id }
    @{ $DAT->{_location_visit_by_user}{ $orig->{user} } };

    splice @{ $DAT->{_location_visit_by_user}{ $orig->{user} } },
        $orig_pos, 1;

    my $orig_posref = \$orig_pos;
    my $orig_ds     = int( $orig_loc->{distance} / 10 );

    _del_index( 'v', $orig->{user} . '_' . $orig_part, $orig_posref );
    _del_index( 'c', $orig->{user} . '_' . $orig_loc->{country},
      $orig_posref );
    _del_index( 'd', $orig->{user} . '_' . $orig_ds, $orig_posref );
    _del_index( 'vc',
      $orig->{user} . '_' . $orig_part . '_' . $orig_loc->{country},
      $orig_posref );
    _del_index( 'vd', $orig->{user} . '_' . $orig_part . '_' . $orig_ds,
      $orig_posref );
    _del_index(
      'vcd',
      $orig->{user} . '_'
          . $orig_part . '_'
          . $orig_loc->{country} . '_'
          . $orig_ds,
      $orig_posref
    );
    _del_index( 'cd',
      $orig->{user} . '_' . $orig_loc->{country} . '_' . $orig_ds,
      $orig_posref );

    for (
      my $i = $orig_pos;
      $i < scalar( @{ $DAT->{_location_visit_by_user}{ $orig->{user} } } );
      $i++
        )
    {
      ${ $DAT->{_location_visit_by_user}{ $orig->{user} }[$i]{p} }--;
    }

    my $cpos = firstidx { $_->{v} > $new->{visited_at} }
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
    my $posref = \$pos;
    my $ds     = int( $loc->{distance} / 10 );

    $DAT->{_location_visit_by_user}{ $new->{user} }[$pos] = {
      p => $posref,
      e => $JSON->encode(
        { mark       => $new->{mark},
          visited_at => $new->{visited_at},
          place      => $loc->{place},
        }
      ),
      v => $new->{visited_at},
      c => $loc->{country},
      d => $loc->{distance},
      i => $id,
    };

    _index( 'v', $new->{user} . '_' . $part,           $posref );
    _index( 'c', $new->{user} . '_' . $loc->{country}, $posref );
    _index( 'd', $new->{user} . '_' . $ds,             $posref );
    _index( 'vc', $new->{user} . '_' . $part . '_' . $loc->{country},
      $posref );
    _index( 'vd', $new->{user} . '_' . $part . '_' . $ds, $posref );
    _index( 'vcd',
      $new->{user} . '_' . $part . '_' . $loc->{country} . '_' . $ds,
      $posref );
    _index( 'cd', $new->{user} . '_' . $loc->{country} . '_' . $ds, $posref );

    if ( $cpos >= 0 ) {
      for (
        my $i = $cpos + 1;
        $i < scalar( @{ $DAT->{_location_visit_by_user}{ $new->{user} } } );
        $i++
          )
      {
        ${ $DAT->{_location_visit_by_user}{ $new->{user} }[$i]{p} }++;
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
    return -2
        if _validate( 'users_visits', $key, $params->{$key} ) == -2;
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
      my $k = _get_index( 'v', $id . '_' . $_ );
      next unless $k && scalar @$k;
      push @$keys, @$k;
    }
  }
  elsif ( $params->{country}
    && !$params->{fromDate}
    && !$params->{toDate}
    && !$params->{toDistance} )
  {
    $keys = _get_index( 'c', $id . '_' . $params->{country} );
  }
  elsif ( $params->{toDistance}
    && !$params->{fromDate}
    && !$params->{toDate}
    && !$params->{country} )
  {
    my @t;
    my $ds = int( $params->{toDistance} / 10 );
    for ( 0 .. $ds ) {
      my $k = _get_index( 'd', $id . '_' . $_ );
      next unless $k && scalar @$k;
      push @t, @$k;
    }

    my @t2 = sort { $$a <=> $$b } @t;
    $keys = \@t2;
  }
  elsif ( ( $params->{fromDate} || $params->{toDate} )
    && $params->{country}
    && !$params->{toDistance} )
  {
    my $part1 = int( ( $params->{fromDate} || 0 ) / 10000000 );
    my $part2 = int( ( $params->{toDate}   || 1600000000 ) / 10000000 );
    for ( $part1 .. $part2 ) {
      my $k = _get_index( 'vc', $id . '_' . $_ . '_' . $params->{country} );
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
        my $k = _get_index( 'vd', $id . '_' . $part . '_' . $_ );
        next unless $k && scalar @$k;
        push @t, @$k;
      }
    }

    my @t2 = sort { $$a <=> $$b } @t;
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
        my $k = _get_index( 'vcd',
          $id . '_' . $part . '_' . $params->{country} . '_' . $_ );
        next unless $k && scalar @$k;
        push @t, @$k;
      }
    }

    my @t2 = sort { $$a <=> $$b } @t;
    $keys = \@t2;
  }
  else {
    my $part1 = 0;
    my $part2 = int( 1600000000 / 10000000 );
    for ( $part1 .. $part2 ) {
      my $k = _get_index( 'v', $id . '_' . $_ );
      next unless $k && scalar @$k;
      push @$keys, @$k;
    }
  }

  my $res = '';

  foreach my $i (@$keys) {
    my $val = $DAT->{_location_visit_by_user}{$id}[$$i];

    next
        if $params->{fromDate}
        && $val->{v} <= $params->{fromDate};
    next
        if $params->{toDate}
        && $val->{v} >= $params->{toDate};
    next
        if defined $params->{country}
        && $val->{c} ne $params->{country};
    next
        if defined $params->{toDistance}
        && $val->{d} >= $params->{toDistance};

    $res .= $val->{e} . ',';
  }

  chop $res;
  return "[$res]";
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
          next
              unless $DAT->{_location_avg}{$id}{$key}{$age}{$gender}[0];
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

  my $dt = DateTime->from_epoch(
    epoch     => $birth_date,
    time_zone => $TZ,
  );
  $DAT->{_years}{$birth_date}
      = $TODAY->clone->subtract_datetime($dt)->years();

  return $DAT->{_years}{$birth_date};
}

sub _index {
  my ( $name, $val, $pos ) = @_;

  my $cpos = bsearchidx { $$_ <=> $$pos }
  @{ $DAT->{"i${name}$val"} };

  if ( $cpos < 0 ) {
    $cpos
        = firstidx { $$_ > $$pos } @{ $DAT->{"i${name}$val"} };
  }

  if ( $cpos < 0 ) {
    push @{ $DAT->{"i${name}$val"} }, $pos;
  }
  else {
    splice @{ $DAT->{"i${name}$val"} }, $cpos, 0, ($pos);
  }

  return;
}

sub _get_index {
  my ( $name, $val ) = @_;

  return $DAT->{"i${name}$val"};
}

sub _del_index {
  my ( $name, $val, $pos ) = @_;

  my $cpos = bsearchidx { $$_ <=> $$pos }
  @{ $DAT->{"i${name}$val"} };

  if ( $cpos >= 0 ) {
    splice @{ $DAT->{"i${name}$val"} }, $cpos, 1;
  }
  else {
    warn 'Del index failed ';
    return -1;
  }

  return;
}

1;
