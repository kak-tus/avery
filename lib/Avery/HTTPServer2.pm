package Avery::HTTPServer2;

use strict;
use warnings;
use v5.10;
use utf8;

use AnyEvent;
use AnyEvent::HTTP::Server;
use Avery::Model::DB;
use Cpanel::JSON::XS;
use Data::Dumper;
use Encode qw( decode_utf8 encode_utf8 );
use EV;
use Log::Fast;
use Time::HiRes qw( gettimeofday tv_interval usleep );

my $httpd;

my %entities = ( users => 1, visits => 1, locations => 1 );

my $logger = Log::Fast->new;

my $db = Avery::Model::DB->new( logger => $logger );
$db->load();

my $JSON = Cpanel::JSON::XS->new->utf8;

my $STAGE = 1;

my %STAT;
my %CACHE;

my $headers = {
  'Content-Type' => 'application/json; charset=utf-8',
  'connection'   => 'Keep-Alive',
};

my $t0;
my @path;
my $pth_len;
my $val;
my $cache_key;
my $fh;
my $req_mtd;
my $pos;

sub run {
  $httpd = AnyEvent::HTTP::Server->new(
    host => '0.0.0.0',
    port => 80,
    cb   => sub {
      my $req = shift;

      if ( $req->method eq 'POST'
        && $req->headers->{'content-length'}
        && $req->headers->{'content-length'} > 0 )
      {
        return HANDLE => sub {
          my $h = $_[0];
          $h->on_read(
            sub {
              my $h = shift;
              process( $req, $h->{rbuf} );
            }
          );
        };
      }
      else {
        process($req);
      }
    }
  );

  $httpd->listen;
  $httpd->accept;

  EV::loop;
}

sub process {
  my ( $req, $content ) = @_;

  $pos = index( $req->uri, '?' );
  $pos = length( $req->uri ) if $pos < 0;

  @path = split '/', substr( $req->uri, 0, $pos );
  $pth_len = @path;

  $req_mtd = $req->method;

  if ( $req_mtd eq 'GET' && $pth_len == 3 ) {
    if ( $STAGE == 2 ) {
      $STAGE = 3;
      undef %CACHE;
      undef %STAT;
    }

    $val = $db->read( $path[1], $path[2] );
    unless ($val) {
      $req->reply( 404, '{}', headers => $headers );
      return;
    }

    $req->reply( 200, $val, headers => $headers );

    return;
  }
  elsif ( $req_mtd eq 'GET' && $pth_len == 4 ) {
    if ( $STAGE == 2 ) {
      $STAGE = 3;
      undef %CACHE;
      undef %STAT;
    }

    if ( $STAGE == 3 ) {
      $req->reply( 200, '{}', headers => $headers );
      return;
    }

    $cache_key = $req->uri;

    $STAT{$cache_key} //= 0;
    $STAT{$cache_key}++;

    if ( $CACHE{$cache_key} ) {
      $req->reply( 200, $CACHE{$cache_key}, headers => $headers );
      return;
    }

    if ( $path[1] eq 'users' ) {
      $val = $db->users_visits( $path[2], $req->params );

      if ( $val eq '-1' ) {
        $req->reply( 404, '{}', headers => $headers );
      }
      elsif ( $val eq '-2' ) {
        $req->reply( 400, '{}', headers => $headers );
      }
      else {
        $val = encode_utf8($val);

        if ( $STAT{$cache_key} > 1 ) {
          $CACHE{$cache_key} = $val;
        }

        $req->reply( 200, $val, headers => $headers );
      }
    }
    elsif ( $path[1] eq 'locations' ) {
      $val = $db->avg( $path[2], $req->params );

      if ( $val == -1 ) {
        $req->reply( 404, '{}', headers => $headers );
      }
      elsif ( $val == -2 ) {
        $req->reply( 400, '{}', headers => $headers );
      }
      else {
        my $enc = qq[{"avg":$val}];

        if ( $STAT{$cache_key} > 1 ) {
          $CACHE{$cache_key} = $enc;
        }

        $req->reply( 200, $enc, headers => $headers );
      }
    }
    else {
      $req->reply( 404, '{}', headers => $headers );
    }
  }
  elsif ( $req_mtd eq 'POST' ) {
    $STAGE = 2;

    if ( $pth_len == 3 && $entities{ $path[1] } && $path[2] eq 'new' ) {
      $val = eval { $JSON->decode( $content // '{}' ) };

      unless ( $val && keys %$val ) {
        $req->reply( 400, '{}', headers => $headers );
        return;
      }

      $val = $db->create( $path[1], $val );

      if ( $val == 1 ) {
        $req->reply( 200, '{}', headers => $headers );
      }
      elsif ( $val == -2 ) {
        $req->reply( 400, '{}', headers => $headers );
      }
    }
    elsif ( $pth_len == 3 && $entities{ $path[1] } && $path[2] =~ m/^\d+$/ ) {
      $val = eval { $JSON->decode( $content // '{}' ) };

      unless ( $val && keys %$val ) {
        $req->reply( 400, '{}', headers => $headers );
        return;
      }

      $val = $db->update( $path[1], $path[2], $val );

      if ( $val == 1 ) {
        $req->reply( 200, '{}', headers => $headers );
      }
      elsif ( $val == -1 ) {
        $req->reply( 404, '{}', headers => $headers );
      }
      elsif ( $val == -2 ) {
        $req->reply( 400, '{}', headers => $headers );
      }
    }
    else {
      $req->reply( 404, '{}', headers => $headers );
    }
  }
  else {
    $req->reply( 404, '{}', headers => $headers );
  }

  return;
}

1;
