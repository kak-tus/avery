#!/usr/bin/env perl

use strict;
use warnings;
use v5.10;
use utf8;

use lib 'lib';

use Avery::HTTPServerPSGI;

return Avery::HTTPServerPSGI->app();
