#!/usr/bin/env perl

use strict;
use warnings;
use v5.10;
use utf8;

use Mojo::Base -strict;

$ENV{MOJO_MODE} = 'development';

unshift( @INC, 'lib' );

require Mojolicious::Commands;
Mojolicious::Commands->start_app('Avery::Mojo');
