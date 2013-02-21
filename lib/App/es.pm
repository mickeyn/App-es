package App::es;
use strict;
use warnings;
use Moo;
use MooX::Options protect_argv => 0;
use ElasticSearch;

use App::es::ParamValidation;

our $VERSION = "0.1";

####

my %commands = (
    ls         => [ qw/ subname / ],
    'ls-types' => [ qw/ index_y / ],

    create     => [ qw/ index_n / ],
    delete     => [ qw/ index_y / ],

    get        => [ qw/ index_y type doc_id / ],
    put        => [ qw/ index_y type json_file / ],

    search     => [ qw/ index_y type searchstr size / ],

    alias      => [ qw/ index_y_notalias alias_n / ],
    unalias    => [ qw/ index_y_notalias alias_y / ],
);

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

sub is_command {
    my ( $class, $cmd ) = @_;
    return ( exists $commands{$cmd} ? 1 : 0 );
}

sub validate_params {
    my ( $self, $cmd, $params ) = @_;
    my @params = @$params;

    my $es = $self->es;

    my $aliases = $es->get_aliases;

    for my $arg_type ( @{ $commands{$cmd} } ) {
        my $param = shift @params;

        my $validator = App::es::ParamValidation->get_validator( $arg_type )
            or die "[ERROR] invalid arg type defined for command: $cmd\n";

        $validator->( $param, $es, $aliases );
    }

    die "[ERROR] too many arguments for command: $cmd\n"
        if @params;
}

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
      INDEX: for ( @indices ) {
            next INDEX if $str and !/$str/;
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

