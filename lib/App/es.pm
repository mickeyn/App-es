package App::es;
use strict;
use warnings;
use Moo;
use ElasticSearch;

our $VERSION = "0.1";

use List::MoreUtils qw{ uniq };

has es => (
    is => "ro",
    isa => sub {
        die "[ERROR] Not a ElasticSearch object." unless ref($_[0]) eq "ElasticSearch"
    },
    required => 1
);

sub validate_params {
    my ($self, $cmd, @params) = @_;

    my $es = $self->es;

    my @aliases = @{ $self->_get_elastic_search_aliases };

    # index_ls
    if ( $cmd eq 'ls' ) {
        die "[ERROR] illegal index name sub-string: " . $params[0] . "\n"
            if $params[0] and $params[0] !~ /^[a-zA-Z0-9_-]+$/;

        return;
    }

    my $index = $params[0];

    # check common params
    unless ( $cmd =~ /^(?:ls)$/ ) {
        die "[ERROR] illegal index name: $index\n"
            unless $index and $index =~ /^[a-zA-Z0-9_-]+$/;
    }

    if ( $cmd =~ /^(?:put|search|get)$/ ) {
        my $type = $params[1];
        die "[ERROR] must provide a valid type\n"
            unless $type and $type =~ /^[a-zA-Z0-9_-]+$/;
    }

    # check existance of an index if applicable
    if ( $cmd =~ /^(?:ls-types|delete|put|search|get|alias|unalias)$/ ) {
        my $check_index = $es->index_exists( index => $index );
        die "[ERROR] index $index does not exists\n" unless $check_index->{ok};

    } elsif ( $cmd eq 'create' ) {
        my $check_index = $es->index_exists( index => $index );
        die "[ERROR] index $index already exists\n" if $check_index->{ok};
    }

    # check document file where applicable
    if ( $cmd eq 'put' ) {
        die "[ERROR] must provide a valid json file\n"
            unless $params[2] and -f $params[2];
    }

    # check document id where applicable
    if ( $cmd eq 'get' ) {
        my $doc_id = $params[2];
        die "[ERROR] must provide a valid doc_id\n"
            unless $doc_id and $doc_id =~ /^[a-zA-Z0-9\/_-]+$/;
    }

    # check alias name where applicable
    if ( $cmd =~ /alias/ ) {
        my $alias = $params[1];
        die "[ERROR] must provide a valid alias name\n"
            unless $alias and $alias =~ /^[a-zA-Z0-9_-]+$/;

        die "[ERROR] $index is an alias\n" if grep { /^$index$/ } @aliases;

        my $check_alias = $es->index_exists( index => $alias );
        if ( $cmd eq 'alias' ) {
            die "[ERROR] alias $alias already exists" if $check_alias->{ok};

        } else {
            die "[ERROR] alias doesn't exist" unless $check_alias->{ok};
        }
    }

    # check search params
    if ( $cmd eq 'search' ) {
        my $string = $params[2];
        my $size   = $params[3];

        utf8::decode($string);
        die "[ERROR] must provide a query string\n"
            unless $string and $string =~ /^[a-zA-Z0-9_-]+ : [\w\d_\s-]+$/x;

        die "[ERROR] invalid size\n"
            if $size and $size !~ /^[0-9]+$/;
    }

    return @params;
}

sub _get_elastic_search_aliases {
    my ($self) = @_;

    my @aliases;
    my $aliases = $self->es->get_aliases;

    if ($ElasticSearch::VERSION < 0.52) {
        @aliases = keys %{$aliases->{aliases}};
    }
    else {
        for my $i (keys %$aliases) {
            push @aliases, keys %{$aliases->{$i}{aliases}};
        }
        @aliases = uniq @aliases;
    }

    return \@aliases;
}

1;
__END__

=head1 NAME

App::es - ElasticSearch command line client.

=head1 DESCRIPTION

Please read the usage and document in the L<es> program.

=head1 AUTHORS

Mickey Nasriachi E<lt>mickey75@gmail.comE<gt>

Kang-min Liu E<lt>gugod@gugod.orgE<gt>


=head1 ACKNOWLEDGMENT

This module was originally developed for Booking.com. With approval from
Booking.com, this module was generalized and published on CPAN, for which the
authors would like to express their gratitude.

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013 by Mickey Nasriachi

Copyright (C) 2013 by Kang-min Liu

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

