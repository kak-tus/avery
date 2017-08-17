#!/usr/bin/env perl

use strict;
use warnings;
use v5.10;
use utf8;

use lib 'lib';

use Avery::HTTPServerPSGI;
use HTTP::Server::PSGI;

my $server = HTTP::Server::PSGI->new(
  host    => '0.0.0.0',
  port    => 80,
  timeout => 20,
);

$server->run( Avery::HTTPServerPSGI->app() );
