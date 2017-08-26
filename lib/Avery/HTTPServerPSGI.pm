package Avery::HTTPServerPSGI;

use strict;
use warnings;
use v5.10;
use utf8;

use Avery::Model::DB;
use Cpanel::JSON::XS;
use Data::Dumper;
use Encode qw( decode_utf8 encode_utf8 );
use Log::Fast;
use Text::QueryString;
use Time::HiRes qw( gettimeofday tv_interval );

my %entities = ( users => 1, visits => 1, locations => 1 );

my $logger = Log::Fast->new;

my $db = Avery::Model::DB->new( logger => $logger );
$db->load();

my $JSON = Cpanel::JSON::XS->new->utf8;

my $STAGE = 1;

my %STAT;
my %CACHE;

my $tqs = Text::QueryString->new;

my $ret_404 = [
  404,
  [ 'Content-length' => 2,
    'Content-Type'   => 'application/json; charset=utf-8',
    'Connection'     => 'Keep-Alive',
  ],
  ['{}']
];

my $ret_400 = [
  400,
  [ 'Content-length' => 2,
    'Content-Type'   => 'application/json; charset=utf-8',
    'Connection'     => 'Keep-Alive',
  ],
  ['{}']
];

my $ret_200 = [
  200,
  [ 'Content-length' => 0,
    'Content-Type'   => 'application/json; charset=utf-8',
    'Connection'     => 'Keep-Alive',
  ],
  ['{}']
];

my $ret_200_ok = [
  200,
  [ 'Content-length' => 2,
    'Content-Type'   => 'application/json; charset=utf-8',
    'Connection'     => 'Keep-Alive',
  ],
  ['{}']
];

my %vars;
my $content;
my $t0;
my @path;
my $pth_len;
my $val;
my $cache_key;
my $fh;
my $req_mtd;

sub app {
  my $self = shift;

  my $app = sub {
    my $req = shift;

    @path = split '/', $req->{PATH_INFO};
    $pth_len = @path;

    $req_mtd = $req->{REQUEST_METHOD};

    if ( $req_mtd eq 'GET' && $pth_len == 3 ) {
      if ( $STAGE == 2 ) {
        $STAGE = 3;
        undef %CACHE;
        undef %STAT;
      }

      $val = $db->read( $path[1], $path[2] );
      unless ($val) {
        return $ret_404;
      }

      $val             = encode_utf8($val);
      $ret_200->[1][1] = length($val);
      $ret_200->[2][0] = $val;
      return $ret_200;
    }
    elsif ( $req_mtd eq 'GET' && $pth_len == 4 ) {
      if ( $STAGE == 2 ) {
        $STAGE = 3;
        undef %CACHE;
        undef %STAT;
      }

      if ( $req->{QUERY_STRING} ) {
        %vars = $tqs->parse( $req->{QUERY_STRING} );

        if ( $vars{country} ) {
          $vars{country} = decode_utf8( $vars{country} );
        }
      }
      else {
        %vars = ();
      }

      $cache_key
          = $req->{PATH_INFO} . '_'
          . join(
        '_', map { $_ . '_' . $vars{$_} }
            sort keys %vars
          );

      $STAT{$cache_key} //= 0;
      $STAT{$cache_key}++;

      if ( $CACHE{$cache_key} ) {
        $ret_200->[1][1] = $CACHE{$cache_key}->[0];
        $ret_200->[2][0] = $CACHE{$cache_key}->[1];
        return $ret_200;
      }

      if ( $path[1] eq 'users' ) {
        $val = $db->users_visits( $path[2], \%vars );

        if ( $val eq '-1' ) {
          return $ret_404;
        }
        elsif ( $val eq '-2' ) {
          return $ret_400;
        }
        else {
          $val             = encode_utf8($val);
          $ret_200->[1][1] = length($val);
          $ret_200->[2][0] = $val;

          if ( $STAT{$cache_key} > 1 ) {
            $CACHE{$cache_key} = [ $ret_200->[1][1], $ret_200->[2][0] ];
          }
          return $ret_200;
        }
      }
      elsif ( $path[1] eq 'locations' ) {
        $val = $db->avg( $path[2], \%vars );

        if ( $val == -1 ) {
          return $ret_404;
        }
        elsif ( $val == -2 ) {
          return $ret_400;
        }
        else {
          my $enc = qq[{"avg":$val}];

          $ret_200->[1][1] = length($enc);
          $ret_200->[2][0] = $enc;

          if ( $STAT{$cache_key} > 1 ) {
            $CACHE{$cache_key} = [ $ret_200->[1][1], $ret_200->[2][0] ];
          }
          return $ret_200;
        }
      }
      else {
        return $ret_404;
      }
    }
    elsif ( $req_mtd eq 'POST' ) {
      $STAGE = 2;

      if ( $req->{CONTENT_LENGTH} ) {
        $fh = $req->{'psgi.input'};

        $fh->seek( 0, 0 );
        $fh->read( $content, $req->{CONTENT_LENGTH}, 0 );
        $fh->seek( 0, 0 );
      }
      else {
        undef $content;
      }

      if ( $pth_len == 3 && $entities{ $path[1] } && $path[2] eq 'new' ) {
        $val = eval { $JSON->decode( $content // '{}' ) };

        unless ( $val && keys %$val ) {
          return $ret_400;
        }

        $val = $db->create( $path[1], $val );

        if ( $val == 1 ) {
          return $ret_200_ok;
        }
        elsif ( $val == -2 ) {
          return $ret_400;
        }
      }
      elsif ( $pth_len == 3 && $entities{ $path[1] } && $path[2] =~ m/^\d+$/ )
      {
        $val = eval { $JSON->decode( $content // '{}' ) };

        unless ( $val && keys %$val ) {
          return $ret_400;
        }

        $val = $db->update( $path[1], $path[2], $val );

        if ( $val == 1 ) {
          return $ret_200_ok;
        }
        elsif ( $val == -1 ) {
          return $ret_404;
        }
        elsif ( $val == -2 ) {
          return $ret_400;
        }
      }
      else {
        return $ret_404;
      }
    }
    else {
      return $ret_404;
    }

  };

  return $app;
}

1;
