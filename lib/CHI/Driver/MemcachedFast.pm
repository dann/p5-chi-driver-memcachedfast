package CHI::Driver::MemcachedFast;
use Cache::Memcached::Fast;
use Carp;
use Moose;
use strict;
use warnings;

extends 'CHI::Driver::Base::CacheContainer';

has 'compress_threshold' => ( is => 'ro' );
has 'debug'              => ( is => 'ro' );
has 'memd'               => ( is => 'ro' );
has 'no_rehash'          => ( is => 'ro' );
has 'servers'            => ( is => 'ro' );
has 'readonly'           => ( is => 'ro' );

has 'connect_timeout'   => ( is => 'ro' );
has 'io_timeout'        => ( is => 'ro' );
has 'close_on_error'    => ( is => 'ro' );
has 'compress_ratio '   => ( is => 'ro' );
has 'compress_methods'  => ( is => 'ro' );
has 'max_failures'      => ( is => 'ro' );
has 'failure_timeout'   => ( is => 'ro' );
has 'ketama_points'     => ( is => 'ro' );
has 'nowait'            => ( is => 'ro' );
has 'hash_namespace'    => ( is => 'ro' );
has 'serialize_methods' => ( is => 'ro' );
has 'utf8'              => ( is => 'ro' );
has 'max_size'          => ( is => 'ro' );
has 'check_args'        => ( is => 'ro' );

__PACKAGE__->meta->make_immutable();

sub BUILD {
    my ( $self, $params ) = @_;

    my %mc_params = (
        map { exists( $self->{$_} ) ? ( $_, $self->{$_} ) : () }
            qw(compress_threshold debug namespace no_rehash servers
            connect_timeout io_timeout close_on_error compress_ratio
            compress_methods max_failures failure_timeout ketama_points
            nowait hash_namespace serialize_methods utf8 max_size check_args
            )
    );
    $self->{_contained_cache} = $self->{memd}
        = Cache::Memcached::Fast->new( \%mc_params );
}

sub clear {
    my ($self) = @_;

    $self->{_contained_cache}->flush_all();
}

# Memcached supports fast multiple get
#

sub get_multi_hashref {
    my ( $self, $keys ) = @_;
    croak "must specify keys" unless defined($keys);

    my $keyvals = $self->{memd}->get_multi(@$keys);
    foreach my $key ( keys(%$keyvals) ) {
        if ( defined $keyvals->{$key} ) {
            $keyvals->{$key} = $self->get( $key, data => $keyvals->{$key} );
        }
    }
    return $keyvals;
}

sub get_multi_arrayref {
    my ( $self, $keys ) = @_;
    croak "must specify keys" unless defined($keys);

    my $keyvals = $self->get_multi_hashref($keys);
    return [ map { $keyvals->{$_} } @$keys ];
}

# Not supported
#

sub get_keys {
    croak "get_keys not supported for " . __PACKAGE__;
}

sub get_namespaces {
    croak "get_namespaces not supported for " . __PACKAGE__;
}

1;

__END__

=pod

=head1 NAME

CHI::Driver::MemcachedFast -- Distributed cache via memcached (memory cache daemon)

=head1 SYNOPSIS

    use CHI;

    my $cache = CHI->new(
        driver => 'MemcachedFast',
        servers => [ "10.0.0.15:11211", "10.0.0.15:11212", "/var/sock/memcached",
        "10.0.0.17:11211", [ "10.0.0.17:11211", 3 ] ],
        debug => 0,
        compress_threshold => 10_000,
    );

=head1 DESCRIPTION

This cache driver uses Cache::Memcached to store data in the specified memcached server(s).

=head1 CONSTRUCTOR OPTIONS

When using this driver, the following options can be passed to CHI->new() in addition to the
L<CHI|general constructor options/constructor>.
    
=over

=item cache_size

=item page_size

=item num_pages

=item init_file

These options are passed directly to L<Cache::Memcached::Fast>.

=back

=head1 METHODS

=over

=item memd

Returns a handle to the underlying Cache::Memcached::Fast object. You can use this to call memcached-specific methods that
are not supported by the general API, e.g.

    $self->memd->incr("key");
    my $stats = $self->memd->stats();

=back

=head1 SEE ALSO

Cache::Memcached
CHI

=head1 AUTHOR

Takatoshi Kitano <kitano.tk at gmail.com> 

=head1 COPYRIGHT & LICENSE

Copyright (C) 2008 Dann

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
