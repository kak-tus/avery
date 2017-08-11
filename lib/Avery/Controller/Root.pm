package Avery::Controller::Root;

use strict;
use warnings;
use v5.10;
use utf8;

use Mojo::Base 'Mojolicious::Controller';

sub routes {
  my $route = shift;

  $route->get('/:entity/:id')->to('Root#read');
  $route->post('/:entity/new')->to('Root#create');
  $route->post('/:entity/:id')->to('Root#update');

  return;
}

sub read {
  my $self = shift;

  my $val = $self->db->read( $self->stash('entity'), $self->stash('id') );

  unless ($val) {
    $self->render( json => {}, status => 404 );
  }

  $self->render( data => $val );

  return;
}

sub update {
  my $self = shift;

  my $status = $self->db->update( $self->stash('entity'),
    $self->stash('id'), $self->req->json );

  if ( $status == 1 ) {
    $self->render( json => {} );
  }
  elsif ( $status == -1 ) {
    $self->render( json => {}, status => 404 );
  }
  elsif ( $status == -2 ) {
    $self->render( json => {}, status => 400 );
  }

  return;
}

sub create {
  my $self = shift;

  my $status = $self->db->create( $self->stash('entity'), $self->req->json );

  if ( $status == 1 ) {
    $self->render( json => {} );
  }
  elsif ( $status == -2 ) {
    $self->render( json => {}, status => 400 );
  }

  return;
}

1;
