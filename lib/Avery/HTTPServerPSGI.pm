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

sub app {
  my $self = shift;

  my $app = sub {
    my $req = shift;
    return _form_req($req);
  };

  return $app;
}

sub _form_req {
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

  my $q = {
    data => {
      method  => $req->{REQUEST_METHOD},
      path    => $req->{PATH_INFO},
      content => $content,
      vars    => \%vars,
    },
    key => $req->{REQUEST_URI},
  };

  return handle_request($q);
}

sub handle_request {
  my $q = shift;

  if ( $q->{data}{method} ne 'POST' && $STAGE == 2 ) {
    $STAGE = 3;
    undef %CACHE;
    undef %STAT;
  }

  return _process($q);
}

sub _process {
  my $q = shift;

  my @path = split '/', $q->{data}{path};

  ## кэш только для долгих запросов
  if ( $q->{data}{method} eq 'GET' && scalar(@path) == 4 ) {
    $STAT{ $q->{key} } //= 0;
    $STAT{ $q->{key} }++;

    if ( $CACHE{$key} ) {
      return [
        $CACHE{$key}->{code},
        [ 'Content-Type'   => 'application/json; charset=utf-8',
          'Content-length' => length( $CACHE{$key}->{data} ),
          'Connection'     => 'close',
        ],
        [ $CACHE{$key}->{data} ]
      ];
    }
  }

  if ( scalar(@path) == 3
    && $entities{ $path[1] }
    && $path[2] eq 'new'
    && $q->{data}{method} eq 'POST' )
  {
    $STAGE = 2;

    my $data = $q->{data}{content};
    my $val = eval { $JSON->decode($data) };

    unless ( $val && keys %$val ) {
      return _400($q);
    }
    my $status = $db->create( $path[1], $val );

    if ( $status == 1 ) {
      return _200( $q, '{}' );
    }
    elsif ( $status == -2 ) {
      return _400($q);
    }
  }
  elsif ( scalar(@path) == 3
    && $entities{ $path[1] }
    && $path[2] =~ m/^\d+$/ )
  {
    if ( $q->{data}{method} eq 'GET' ) {
      my $val = $db->read( $path[1], $path[2] );

      unless ($val) {
        return _404($q);
      }

      return _200( $q, $val );
    }
    elsif ( $q->{data}{method} eq 'POST' ) {
      $STAGE = 2;

      my $data = $q->{data}{content};
      my $val = eval { $JSON->decode($data) };

      unless ( $val && keys %$val ) {
        return _400($q);
      }

      my $status = $db->update( $path[1], $path[2], $val );

      if ( $status == 1 ) {
        return _200( $q, '{}' );
      }
      elsif ( $status == -1 ) {
        return _404($q);
      }
      elsif ( $status == -2 ) {
        return _400($q);
      }
    }
    else {
      return _404($q);
    }
  }
  elsif ( scalar(@path) == 4
    && $path[1] eq 'users'
    && $path[2] =~ m/^\d+$/
    && $path[3] eq 'visits'
    && $q->{data}{method} eq 'GET' )
  {
    my %args;
    foreach (qw( fromDate toDate country toDistance )) {
      next unless defined $q->{data}{vars}{$_};
      $args{$_} = $q->{data}{vars}{$_};
    }

    my $vals = $db->users_visits( $path[2], %args );

    if ( $vals == -1 ) {
      return _404($q);
    }
    elsif ( $vals == -2 ) {
      return _400($q);
    }
    else {
      return _200( $q, $JSON->encode( { visits => $vals } ) );
    }
  }
  elsif ( scalar(@path) == 4
    && $path[1] eq 'locations'
    && $path[2] =~ m/^\d+$/
    && $path[3] eq 'avg'
    && $q->{data}{method} eq 'GET' )
  {
    my %args;
    foreach (qw( fromDate toDate fromAge toAge gender )) {
      next unless defined $q->{data}{vars}{$_};
      $args{$_} = $q->{data}{vars}{$_};
    }

    my $avg = $db->avg( $path[2], %args );

    if ( $avg == -1 ) {
      return _404($q);
    }
    elsif ( $avg == -2 ) {
      return _400($q);
    }
    else {
      return _200( $q, qq[{"avg":$avg}] );
    }
  }
  else {
    return _404($q);
  }

  return;
}

sub _200 {
  my ( $q, $data ) = @_;

  return _store( $q, 200, $data );
}

sub _404 {
  my $q = shift;

  return _store( $q, 404, '{}' );
}

sub _400 {
  my $q = shift;

  return _store( $q, 400, '{}' );
}

sub _store {
  my ( $q, $code, $data ) = @_;

  if ( $q->{key} && $STAT{ $q->{key} } > 1 ) {
    $CACHE{ $q->{key} } = { code => $code, data => $data };
  }

  return [
    $code,
    [ 'Content-Type'   => 'application/json; charset=utf-8',
      'Content-length' => length($data),
      'Connection'     => 'close',
    ],
    [$data]
  ];

  return;
}

1;
