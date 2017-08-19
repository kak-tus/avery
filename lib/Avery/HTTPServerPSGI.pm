package Avery::HTTPServerPSGI;

use strict;
use warnings;
use v5.10;
use utf8;

use Avery::Model::DB;
use Cpanel::JSON::XS;
use Data::Dumper;
use Encode qw(decode_utf8);
use Log::Fast;
use Text::QueryString;

my %entities = ( users => 1, visits => 1, locations => 1 );

my $logger = Log::Fast->new;

my $db = Avery::Model::DB->new( logger => $logger );
$db->load();

my $JSON = Cpanel::JSON::XS->new->utf8;

my $STAGE = 1;

my %FORKS;

my %STAT;
my %CACHE;

my $PIPE_RESP;

my $tqs = Text::QueryString->new;

my $headers = [
  'Content-length' => 0,
  'Content-Type'   => 'application/json; charset=utf-8',
  'Connection'     => 'close',
];

sub app {
  my $self = shift;

  my $app = sub {
    my $req = shift;

    my %vars;
    if ( $req->{QUERY_STRING} ) {
      %vars = $tqs->parse( $req->{QUERY_STRING} );

      if ( $vars{country} ) {
        $vars{country} = decode_utf8( $vars{country} );
      }
    }

    my $content;
    if ( $req->{CONTENT_LENGTH} ) {
      my $fh = $req->{'psgi.input'};
      my $cl = $req->{CONTENT_LENGTH};

      $fh->seek( 0, 0 );
      $fh->read( $content, $cl, 0 );
      $fh->seek( 0, 0 );
    }

    my $q = {};

    if ( $req->{REQUEST_METHOD} ne 'POST' && $STAGE == 2 ) {
      $STAGE = 3;
      undef %CACHE;
      undef %STAT;
    }

    my @path = split '/', $req->{PATH_INFO};

    ## кэш только для долгих запросов
    if ( $req->{REQUEST_METHOD} eq 'GET' && scalar(@path) == 4 ) {
      $q->{key} = $req->{REQUEST_URI};
      $STAT{ $q->{key} }++;

      if ( $CACHE{ $q->{key} } ) {
        $headers->[1] = length( $CACHE{ $q->{key} }->{data} );
        return [
          $CACHE{ $q->{key} }->{code},
          $headers,
          [ $CACHE{ $q->{key} }->{data} ]
        ];
      }
    }

    if ( scalar(@path) == 3
      && $entities{ $path[1] }
      && $path[2] eq 'new'
      && $req->{REQUEST_METHOD} eq 'POST' )
    {
      $STAGE = 2;

      my $val = eval { $JSON->decode($content) };

      unless ( $val && keys %$val ) {
        return _store( $q, 400, '{}' );
      }
      my $status = $db->create( $path[1], $val );

      if ( $status == 1 ) {
        return _store( $q, 200, '{}' );
      }
      elsif ( $status == -2 ) {
        return _store( $q, 400, '{}' );
      }
    }
    elsif ( scalar(@path) == 3
      && $entities{ $path[1] }
      && $path[2] =~ m/^\d+$/ )
    {
      if ( $req->{REQUEST_METHOD} eq 'GET' ) {
        my $val = $db->read( $path[1], $path[2] );

        unless ($val) {
          return _store( $q, 404, '{}' );
        }

        return _store( $q, 200, $val );
      }
      elsif ( $req->{REQUEST_METHOD} eq 'POST' ) {
        $STAGE = 2;

        my $val = eval { $JSON->decode($content) };

        unless ( $val && keys %$val ) {
          return _store( $q, 400, '{}' );
        }

        my $status = $db->update( $path[1], $path[2], $val );

        if ( $status == 1 ) {
          return _store( $q, 200, '{}' );
        }
        elsif ( $status == -1 ) {
          return _store( $q, 404, '{}' );
        }
        elsif ( $status == -2 ) {
          return _store( $q, 400, '{}' );
        }
      }
      else {
        return _store( $q, 404, '{}' );
      }
    }
    elsif ( scalar(@path) == 4
      && $path[1] eq 'users'
      && $path[2] =~ m/^\d+$/
      && $path[3] eq 'visits'
      && $req->{REQUEST_METHOD} eq 'GET' )
    {
      my $vals = $db->users_visits( $path[2], \%vars );

      if ( $vals == -1 ) {
        return _store( $q, 404, '{}' );
      }
      elsif ( $vals == -2 ) {
        return _store( $q, 400, '{}' );
      }
      else {
        return _store( $q, 200, $JSON->encode( { visits => $vals } ) );
      }
    }
    elsif ( scalar(@path) == 4
      && $path[1] eq 'locations'
      && $path[2] =~ m/^\d+$/
      && $path[3] eq 'avg'
      && $req->{REQUEST_METHOD} eq 'GET' )
    {
      my $avg = $db->avg( $path[2], \%vars );

      if ( $avg == -1 ) {
        return _store( $q, 404, '{}' );
      }
      elsif ( $avg == -2 ) {
        return _store( $q, 400, '{}' );
      }
      else {
        return _store( $q, 200, qq[{"avg":$avg}] );
      }
    }
    else {
      return _store( $q, 404, '{}' );
    }
  };

  return $app;
}

sub _store {
  my ( $q, $code, $data ) = @_;

  if ( $q->{key} ) {
    $CACHE{ $q->{key} } = { code => $code, data => $data };
  }

  $headers->[1] = length($data);
  return [ $code, $headers, [$data] ];

  return;
}

1;
