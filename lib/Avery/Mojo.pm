package Avery::Mojo;

use strict;
use warnings;
use v5.10;
use utf8;

use Mojo::Base 'Mojolicious';

use Avery::Controller::Root;
use Avery::Model::DB;

my $JSON = Cpanel::JSON::XS->new;

sub startup {
  my $self = shift;

  $self->mode( $ENV{MOJO_MODE} // 'production' );
  $self->secrets(1);

  my $route = $self->routes();
  $route->namespaces( ['Avery::Controller'] );

  Avery::Controller::Root::routes($route);

  $self->helper(
    db => sub {
      state $db;
      return $db if $db;

      $db = Avery::Model::DB->new;
      return $db;
    }
  );

  $self->db->load();

  $self->config(
    { hypnotoad => {
        listen  => ['http://0.0.0.0:80'],
        workers => 1,
        accepts => 0,
      }
    }
  );

  return;
}

1;
