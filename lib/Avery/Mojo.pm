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

  my $pid = $$;
  $self->helper(
    db => sub {
      state $db;
      return $db if $db;

      $db = Avery::Model::DB->new( parent_pid => $pid, logger => $self->log );
      return $db;
    }
  );

  $self->db->load();

  $self->config(
    { hypnotoad => {
        listen  => ['http://0.0.0.0:80'],
        workers => 1,
        accepts => 0,
        clients => 5000,
        ## backlog            => 65535,
        heartbeat_interval => 30,
        heartbeat_timeout  => 30,
        inactivity_timeout => 30,
      }
    }
  );

  return;
}

1;
