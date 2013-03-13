package App::es;
use strict;
use warnings;

use App::es::ParamValidation qw(ESExistingIndex ESName);
use JSON qw( decode_json to_json );
use ElasticSearch;
use File::Slurp qw(read_file);
use Term::ANSIColor;
use URI;
use URI::Split qw(uri_split);

use Moo;
use MooX::Options protect_argv => 0;
use MooX::Types::MooseLike::Base qw(:all);

our $VERSION = "0.1";

####
####

my %commands = (
    ls           => [ qw/ subname_opt / ],
    'ls-types'   => [ qw/ index_y / ],
    'ls-aliases' => [ qw/ index_y / ],

    create     => [ qw/ index_n / ],
    delete     => [ qw/ index_y / ],
    reindex    => [ qw/ index_y index_y / ],
    copy       => [ qw/ index_fq index_fq / ],

    get        => [ qw/ index_y type doc_id / ],
    put        => [ qw/ index_y type json_file / ],

    'get-mapping'  => [ qw/ index_y / ],
    'get-settings' => [ qw/ index_y / ],

    'put-mapping'  => [ qw/ index_y json_file / ],
    'put-settings' => [ qw/ index_y json_file / ],

    search     => [ qw/ searchstr / ],
    scan       => [ qw/ index_y type string / ],

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

option size => (
    is => "ro",
    format => "i",
    documentation => "The size of search result.",
    default => sub { 24 },
    isa => App::es::ParamValidation->get_validator("size"),
);

option index => (
    is => "ro",
    format => "s@",
    documentation => "The index/indices for doing searches.",
    isa => ArrayRef[ESName],
);

option type => (
    is => "ro",
    format => "s@",
    documentation => "The document types for doing searches.",
    isa => App::es::ParamValidation->get_validator("type"),
    isa => ArrayRef[ESName]
);

sub _build_es {
    return ElasticSearch->new(
        servers     => $ENV{ELASTIC_SEARCH_SERVERS},
        trace_calls => $ENV{ELASTIC_SEARCH_TRACE},
        transport   => $ENV{ELASTIC_SEARCH_TRANSPORT} || ($^V gt v5.14.0 ? "httptiny" : "http"),
        timeout     => $ENV{ELASTIC_SEARCH_TIMEOUT} || "10", # seconds
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

    for my $arg_type ( @{ $commands{$cmd} } ) {
        my $param = shift @params;

        my $validator = App::es::ParamValidation->get_validator( $arg_type )
            or die "[ERROR] invalid arg type defined for command: $cmd\n";

        $validator->( $param, $self );
    }

    die "[ERROR] too many arguments for command: $cmd\n"
        if @params;
}

sub command_ls {
    my ($self, $str) = @_;

    my $es = $self->es;

    my $aliases = $es->get_aliases;

    my @indices = $ElasticSearch::VERSION < 0.52
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
    print to_json(
        $result->{$index},
        { pretty => 1 }
    );
}

sub command_get_settings {
    my ($self, $index) = @_;
    my $result = $self->es->index_settings(index => $index);

    my $settings = $result->{$index}{settings};
    print to_json($settings, { pretty => 1 });
}

sub command_put_settings {
    my ($self, $index, $doc ) = @_;
    my $settings = decode_json( read_file($doc) );

    my $result = $self->es->update_index_settings(
        index    => $index,
        settings => $settings
    );
    warn "[ERROR] failed to update index settings\n" unless $result->{ok};
}

sub command_create {
    my ( $self, $index ) = @_;

    my ($settings, $mapping);
    $settings = decode_json( read_file($self->settings) ) if $self->settings;
    $mapping  = decode_json( read_file($self->mapping)  ) if $self->mapping;

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
        bulk_size  => $self->size,
    );
}

sub command_get {
    my ( $self, $index, $type, $doc_id ) = @_;

    my $get = $self->es->get(
        index => $index,
        type  => $type,
        id    => $doc_id,
    );
    return unless exists $get->{_source};
    print to_json($get->{_source}, { pretty => 1 }), "\n";
}

sub command_search {
    my ( $self, $string ) = @_;

    my $query = {
        query_string => {
            query => $string,
            default_field => "_all"
        }
    };

    my @highlight = (
        highlight => {
            fields => { _all => {} },
            pre_tags  => [ '__STARTCOLOR__' ],
            post_tags => [ '__ENDCOLOR__' ],
        }
    );

    my $result = $self->es->search(
        ($self->index ? (index => $self->index) : () ),
        ($self->type  ? (type  => $self->type)  : () ),
        size  => $self->size,
        query => $query,
        @highlight,
    );

    for my $hit (@{ $result->{hits}{hits} }) {
        printf "%s\n", colored($hit->{_id}, 'cyan');
        for my $field (keys %{$hit->{highlight}}) {
            printf "  %s:\n", $field;
            for my $line (@{$hit->{highlight}{$field}}) {
                $line =~ s/\n/ /g;
                $line =~ s/__STARTCOLOR__/color 'bold red'/eg;
                $line =~ s/__ENDCOLOR__/color 'reset'/eg;
                printf "    - %s\n", $line;
            }
        }
    }
}

sub command_scan {
    my ( $self, $index, $type, $string, $size ) = @_;

    my $query = {
        query_string => {
            query => $string,
        }
    };

    my $scroller = $self->es->scrolled_search(
        index       => $index,
        query       => $query,
        search_type => 'scan',
        scroll      => '5m',
    );

    while ( my $n = $scroller->next() ) {
        last unless $size-- > 0;
        print $n->{_id}, "\n";
    }
}

sub command_delete {
    my ( $self, $index ) = @_;

    my $result = $self->es->delete_index(
        index => $index,
        ignore_missing => 1,
    );
    warn "[ERROR] failed to delete index: $index\n" unless $result->{ok};
}

sub command_put {
    my ( $self, $index, $type, $doc ) = @_;

    my $json;
    eval {
        $json = decode_json( read_file($doc) );
        1;
    } or do {
        die "[ERROR] invlid json data in $doc\n";
    };

    $self->es->index(
        index => $index,
        type  => $type,
        data  => $json,
        create => 1
    );
}

sub command_alias {
    my ( $self, $index, $alias ) = @_;

    my $result = $self->es->aliases( actions => [
        { add => { index => $index, alias => $alias } }
    ] );
    warn "[ERROR] failed to create alias $alias for index $index\n" unless $result->{ok};
}

sub command_unalias {
    my ( $self, $index, $alias ) = @_;

    my $result = $self->es->aliases( actions => [
        { remove => { index => $index, alias => $alias } }
    ] );
    warn "[ERROR] failed to remove alias $alias for index $index\n" unless $result->{ok};
}

sub command_copy {
    my ( $self, $index_fq_src, $index_fq_dest ) = @_;
    my $source = $self->_es_from_url( $index_fq_src );
    my $destination = $self->_es_from_url( $index_fq_dest );

    my $res = $destination->{es}->index_exists( index => $destination->{index} );

    if ($source->{hostport} eq $destination->{hostport}) {
        if ($source->{index} eq $destination->{index}) {
            die "[ERROR] Copying an index to itself -- that is not going to work.\n";
        }
    }
    if ($res) {
        die "[ERROR] Destinationination index <$destination->{index}> already exists\n";
    }

    print "Creating destination index <$destination->{index}> with the same settings/mappings\n";
    my $settings = $source->{es}->index_settings( index => $source->{index} )->{$source->{index}}{settings};
    my $mappings = $source->{es}->mapping( index => $source->{index} )->{$source->{index}};
    $destination->{es}->create_index(
        index => $destination->{index},
        settings => $settings,
        mappings => $mappings
    );
    print "Copying documents over.\n";
    my $t0 = time;
    $destination->{es}->reindex(
        source => $source->{es}->scrolled_search(
            query => { match_all => {} },
            search_type => "scan",
            scroll => "10m",
            index => $source->{index}
        ),
        dest_index => $destination->{index}
    );

    my $t0_elapsed = time - $t0;
    print "Done. took $t0_elapsed seconds\n";
}

#### Non-command handlers

sub _es_from_url {
    my ($self, $url) = @_;

    my ($scheme, $hostport, $path, undef, undef) = uri_split($url);

    my $index = $path;
    $index =~ s{^/}{};
    $index =~ s{/$}{};

    if (index($index, "/") >= 0) {
        warn "<$path> looks like index_name/index_type. Removing the index_type part\n";
        $index =~ s{/.*$}{};
    }

    my $es = ElasticSearch->new(
        servers => $hostport,
        timeout => 0,
        max_requests => 0,
        no_refresh => 1
    );

    return {
        es    => $es,
        index => $index,
        hostport => $hostport
    }
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
    my $aliases = $self->es->get_aliases;
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

