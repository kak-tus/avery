package Avery::Controller::Root;

use strict;
use warnings;
use v5.10;
use utf8;

use Mojo::Base 'Mojolicious::Controller';

sub routes {
  my $route = shift;

  $route->get('/locations/:id/avg')->to('Root#avg');
  $route->get('/users/:id/visits')->to('Root#users_visits');
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
    return;
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

sub users_visits {
  my $self = shift;

  my %args = (
    fromDate   => $self->param('fromDate'),
    toDate     => $self->param('toDate'),
    country    => $self->param('country'),
    toDistance => $self->param('toDistance'),
  );

  my $vals = $self->db->users_visits( $self->stash('id'), %args );

  if ( $vals == -1 ) {
    $self->render( json => {}, status => 404 );
  }
  elsif ( $vals == -2 ) {
    $self->render( json => {}, status => 400 );
  }
  else {
    $self->render( json => { visits => $vals } );
  }

  return;
}

sub avg {
  my $self = shift;

  $self->render( json => { avg => 1 } );

  return;
}

1;
