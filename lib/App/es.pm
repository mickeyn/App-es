package App::es;
use strict;
use warnings;
use JSON;
use Moo;
use MooX::Options protect_argv => 0;
use Hash::Flatten qw(unflatten);
use ElasticSearch;
use File::Slurp qw(read_file);
use App::es::ParamValidation;
use Term::ANSIColor;

our $VERSION = "0.1";

####

my %commands = (
    ls           => [ qw/ subname / ],
    'ls-types'   => [ qw/ index_y / ],
    'ls-aliases' => [ qw/ index_y / ],

    create     => [ qw/ index_n / ],
    delete     => [ qw/ index_y / ],
    reindex    => [ qw/ index_y index_y / ],

    get        => [ qw/ index_y type doc_id / ],
    put        => [ qw/ index_y type json_file / ],

    'get-mapping'  => [ qw/ index_y / ],
    'get-settings' => [ qw/ index_y / ],

    'put-mapping'  => [ qw/ index_y json_file / ],
    'put-settings' => [ qw/ index_y json_file / ],

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

option settings => (
    is => "ro",
    format => "s",
    documentation => "The settings json file.",
    isa => App::es::ParamValidation->get_validator("json_file")
);

option mapping => (
    is => "ro",
    format => "s",
    documentation => "The mapping json file.",
    isa => App::es::ParamValidation->get_validator("json_file")
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

sub command_ls_aliases {
    my ( $self, $index ) = @_;

    my $aliases = $self->_get_elastic_search_index_alias_mapping;
    return unless ref($aliases) eq 'HASH' and exists $aliases->{$index};

    print "$_\n" for @{ $aliases->{$index} };
}

sub command_get_mapping {
    my ($self, $index) = @_;
    my $result = $self->es->mapping(index => $index);
    print JSON::to_json(
        $result->{$index},
        { pretty => 1 }
    );
}

sub command_get_settings {
    my ($self, $index) = @_;
    my $result = $self->es->index_settings(index => $index);

    my $settings = $result->{$index}{settings};
    print JSON::to_json($settings, { pretty => 1 });
}

sub command_put_settings {
    my ($self, $index, $doc ) = @_;
    my $settings = JSON::decode_json read_file $doc;

    my $result = $self->es->update_index_settings(
        index    => $index,
        settings => $settings
    );
    warn "[ERROR] failed to update index settings\n" unless $result->{ok};
}

sub command_create {
    my ( $self, $index ) = @_;

    my ($settings, $mapping);
    $settings = JSON::decode_json( read_file($self->settings) ) if $self->settings;
    $mapping  = JSON::decode_json( read_file($self->mapping)  ) if $self->mapping;

    my $result;
    eval {
        $result = $self->es->create_index(
            index => $index,
            $settings ? ( settings => $settings ) : (),
            $mapping  ? ( mappings => $mapping  ) : (),
        );
        1;
    };

    warn "[ERROR] failed to create index: $index\n"
        unless ref($result) eq 'HASH' and $result->{ok};
}

sub command_reindex {
    my ( $self, $index_src, $index_dest ) = @_;
    my $es = $self->es;
    $es->reindex(
        source => $es->scrolled_search(
            query => { match_all => {} },
            search_type => "scan",
            scroll => "10m",
            index  => $index_src,
        ),
        dest_index => $index_dest,
    );
}

sub command_get {
    my ( $self, $index, $type, $doc_id ) = @_;

    my $get = $self->es->get(
        index => $index,
        type  => $type,
        id    => $doc_id,
    );

    print to_json($get->{_source}, { pretty => 1 }), "\n";
}

sub command_search {
    my ( $self, $index, $type, $string, $size ) = @_;

    my ( $field, $text ) = split q{:} => $string;
    my $query = {
        query_string => {
            default_field => $field,
            query         => $text,
        }
    };

    my $result = $self->es->search(
        index => $index,
        type  => $type,
        size  => $size || 24,
        query => $query,
        highlight => { fields => { $field => {} },
                       pre_tags  => [ '__STARTCOLOR__' ],
                       post_tags => [ '__ENDCOLOR__' ],
                     },
    );

    my @output =
        map { { id => $_->{_id},
                lines => $_->{highlight}{$field},
              }
            }
        @{ $result->{hits}{hits} };

    for my $o ( @output ) {
        for my $line ( @{ $o->{lines} } ) {
            $line =~ s/\n/ /g;
            $line =~ s/__STARTCOLOR__/color 'bold red'/eg;
            $line =~ s/__ENDCOLOR__/color 'reset'/eg;
            printf "%s: %s\n", colored ($o->{id}, 'cyan'), $line;
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

