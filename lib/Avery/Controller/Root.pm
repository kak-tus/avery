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

  $self->render( json => $val );

  return;
}

sub update {
  my $self = shift;

  my $val = $self->req->json;
  unless ( $val && keys %$val ) {
    $self->render( json => {}, status => 400 );
    return;
  }

  my $status
      = $self->db->update( $self->stash('entity'), $self->stash('id'), $val );

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

  my $val = $self->req->json;
  unless ( $val && keys %$val ) {
    $self->render( json => {}, status => 400 );
    return;
  }

  my $status = $self->db->create( $self->stash('entity'), $val );

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

  my %args;
  foreach (qw( fromDate toDate country toDistance )) {
    next unless defined $self->param($_);
    $args{$_} = $self->param($_);
  }

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

  my %args;
  foreach (qw( fromDate toDate fromAge toAge gender )) {
    next unless defined $self->param($_);
    $args{$_} = $self->param($_);
  }

  my $avg = $self->db->avg( $self->stash('id'), %args );

  if ( $avg == -1 ) {
    $self->render( json => {}, status => 404 );
  }
  elsif ( $avg == -2 ) {
    $self->render( json => {}, status => 400 );
  }
  else {
    $self->render( json => { avg => $avg } );
  }

  return;
}

1;
