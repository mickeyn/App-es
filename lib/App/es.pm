package App::es;
use strict;
use warnings;
use Moo;
use MooX::Options protect_argv => 0;
use ElasticSearch;

our $VERSION = "0.1";

####

sub commands {
    return qw/
                 alias
                 create
                 delete
                 get
                 ls
                 ls-types
                 put
                 search
                 unalias
             /;
}

#### attributes

has es => (
    is => "lazy",
);

has _aliases => ( is => "lazy" );

option long => (
    is => "ro",
    short => "l",
    default => sub { 0 },
    documentation => "Long format in the output."
);

sub _build_es {
    return ElasticSearch->new(
        servers     => $ENV{ELASTIC_SEARCH_SERVERS},
        trace_calls => $ENV{ELASTIC_SEARCH_TRACE},
    );
}

sub _build__aliases {
    my $self = shift;
    return $self->_get_elastic_search_aliases;
}

#### command handlers

sub command_ls {
    my ($self, $str) = @_;

    my $es = $self->es;

    my $aliases = $es->get_aliases;

    my @indices =$ElasticSearch::VERSION < 0.52
        ? keys %{ $aliases->{indices} }
        : keys %{ $aliases };

    if ($self->long) {
        my $stats;
        eval {
            $stats = $es->index_stats(
                index => \@indices,
                docs  => 1,
                store => 1,
            );
            1;
        } or do {
            die "[ERROR] failed to obtain stats for index: $str\n";
        };


    INDEX: for my $i ( keys %{ $stats->{_all}{indices} } ) {
            next INDEX if $str and $i !~ /$str/;

            my $size  = $stats->{_all}{indices}{$i}{primaries}{store}{size};
            my $count = $stats->{_all}{indices}{$i}{primaries}{docs}{count};

            printf "%s\t%s\t%s\n", $size, $count, $i;
        }
    }
    else {
        for (@indices) {
            print "$_\n";
        }
    }
}

sub command_ls_types {
    my ( $self, $index ) = @_;

    my $es = $self->es;

    my @indices = ( $index );
    my @aliases = @{ $self->_aliases };

    if ( grep { $index eq $_ } @aliases ) {
        my $x = $self->_get_elastic_search_index_alias_mapping;
        @indices = @{ $x->{$index} };
    }

    for my $n (@indices) {
        my $mapping = $es->mapping( index => $n );

    TYPE: for my $type ( keys %{ $mapping->{$n} } ) {
            if ($self->long) {
                my $search = $es->count(
                    index => $n,
                    type  => $type,
                );

                printf "%d\t%s\n", $search->{count}, $type;
            }
            else {
                printf "%s\n", $type;
            }
        }
    }
}

#### Non-command handlers

sub validate_params {
    my ($self, $cmd, @params) = @_;

    my $es = $self->es;

    my @aliases = @{ $self->_aliases };

    # index_ls
    if ( $cmd eq 'ls' ) {
        die "[ERROR] illegal index name sub-string: " . $params[0] . "\n"
            if $params[0] and $params[0] !~ /^[a-zA-Z0-9_-]+$/;

        return;
    }

    my $index = $params[0];

    # check common params
    unless ( $cmd =~ /^(?:ls)$/ ) {
        die "[ERROR] Missing index\n"
            unless $index;
        die "[ERROR] illegal index name: $index\n"
            unless $index =~ /^[a-zA-Z0-9_-]+$/;
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
        my %uniq_aliases;
        for my $i (keys %$aliases) {
            $uniq_aliases{$_} = 1 for keys %{$aliases->{$i}{aliases}};
        }
        @aliases = keys %uniq_aliases;
    }

    return \@aliases;
}

sub _get_elastic_search_index_alias_mapping {
    my ($self) = @_;
    my $es = $self->es;
    my $aliases = $es->get_aliases;
    my %mapping;

    if ($ElasticSearch::VERSION < 0.52) {
        for (keys %{$aliases->{indices}}) {
            push @{ $mapping{$_} ||=[] }, @{ $aliases->{indices}{$_} };
        }
        for (keys %{$aliases->{aliases}}) {
            push @{ $mapping{$_} ||=[] }, @{ $aliases->{aliases}{$_} };
        }
    }
    else {
        for my $i (keys %$aliases) {
            for my $a (@{ $mapping{$i} = [keys %{$aliases->{$i}{aliases}}] }) {
                push @{$mapping{$a} ||=[]}, $i;
            }
        }
    }
    return \%mapping;
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

