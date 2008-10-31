#!perl -w
use strict;
use warnings;
use lib 't/lib';
use CHI::t::Driver::MemcachedFast;
CHI::t::Driver::MemcachedFast->runtests;
