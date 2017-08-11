package Avery::Model::DB;

use strict;
use warnings;
use v5.10;
use utf8;

use Cpanel::JSON::XS;
use RedisDB;

my $JSON = Cpanel::JSON::XS->new->utf8;
my $REDIS;

my %VALIDATION = (
  id         => { min => 1, max => 2147483647 },
  email      => { len => 100 },
  first_name => { len => 50 },
  last_name  => { len => 50 },
  gender => { in => { m => 1, f => 1 } },
  birth_date => { min => -1262304000, max => 915235199 },
  country    => { len => 50 },
  city       => { len => 50 },
  id         => { min => 0,           max => 2147483647 },
  location   => { min => 0,           max => 2147483647 },
  user       => { min => 0,           max => 2147483647 },
  visited_at => { min => 946684800,   max => 1420156799 },
  mark       => { min => 0,           max => 5 },
);

sub new {
  $REDIS = RedisDB->new( path => '/var/run/redis/redis.sock' );

  return bless {};
}

sub load {
  my $self = shift;

  my @files = glob '/tmp/data/*.json';

  foreach my $file (@files) {
    say $file;

    open my $fl, "$file";
    my $st = <$fl>;
    close $fl;

    my $decoded = $JSON->decode($st);

    my $entity = ( keys %$decoded )[0];
    say $entity;

    foreach my $val ( @{ $decoded->{$entity} } ) {
      $self->create( $entity, $val );
    }
  }

  say 'Loaded';

  return;
}

sub create {
  my $self = shift;
  my ( $entity, $val ) = @_;

  foreach my $key ( keys %$val ) {
    next unless $VALIDATION{$key};

    if ( $VALIDATION{$key}->{len} ) {
      return -2 if length( $val->{$key} ) > $VALIDATION{$key}->{len};
    }
    elsif ( $VALIDATION{$key}->{in} ) {
      return -2 unless $VALIDATION{$key}->{in}->{ $val->{$key} };
    }
    elsif ( $VALIDATION{$key}->{max} ) {
      return -2 if $val->{$key} !~ m/^\d+$/;
      return -2 if $val->{$key} < $VALIDATION{$key}->{min};
      return -2 if $val->{$key} > $VALIDATION{$key}->{max};
    }
  }

  my $encoded = $JSON->encode($val);
  $REDIS->set( 'val_' . $entity . '_' . $val->{id}, $encoded );

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
      if ( $VALIDATION{$key}->{len} ) {
        return -2 if length( $val->{$key} ) > $VALIDATION{$key}->{len};
      }
      elsif ( $VALIDATION{$key}->{in} ) {
        return -2 unless $VALIDATION{$key}->{in}->{ $val->{$key} };
      }
      elsif ( $VALIDATION{$key}->{max} ) {
        return -2 if $val->{$key} !~ m/^\d+$/;
        return -2 if $val->{$key} < $VALIDATION{$key}->{min};
        return -2 if $val->{$key} > $VALIDATION{$key}->{max};
      }
    }

    $decoded->{$key} = $val->{$key};
  }

  my $encoded = $JSON->encode($decoded);
  $REDIS->set( 'val_' . $entity . '_' . $id, $encoded );

  return 1;
}

1;
