package ObjectDB::Util;

use strict;
use warnings;

use base 'Exporter';

our $VERSION   = '3.12';
our @EXPORT_OK = qw(execute merge merge_rows filter_columns);

use Carp ();
use Hash::Merge ();
use ObjectDB::Exception;

sub execute {
    my ($dbh, $stmt, %context) = @_;

    my $sql  = $stmt->to_sql;
    my @bind = $stmt->to_bind;

    my ($rv, $sth);
    eval {
        $sth = $dbh->prepare($sql);
        $rv  = $sth->execute(@bind);

        1;
    } or do {
        my $e = $@;

        ObjectDB::Exception->throw($e, %context, sql => $stmt);
    };

    return wantarray ? ($rv, $sth) : $rv;
}

my $merge;

sub merge {
    $merge ||= do {
        my $merge = Hash::Merge->new();
        $merge->set_behavior('STORAGE_PRECEDENT');
        $merge->set_clone_behavior(1);
        $merge;
    };
    $merge->merge(@_);
}

sub merge_rows {
    my $rows = shift;

    my $merged = [];

    my %order;
  NEXT_MERGE: while (@$rows) {
        my $row = shift @$rows;

        my $row_sign = '';
        foreach my $key (sort keys %$row) {
            my $value = $row->{$key};
            $value = \'join' if ref $value eq 'HASH' || ref $value eq 'ARRAY';

            $value = \undef unless defined $value;
            $row_sign .= "$key=$value";
        }

        if (!exists $order{$row_sign}) {
            $order{$row_sign} = $row;

            push @$merged, $row;
            next NEXT_MERGE;
        }

        my $prev = $order{$row_sign};

        foreach my $key (keys %$row) {
            next
              unless ref $prev->{$key} eq 'HASH'
              || ref $prev->{$key} eq 'ARRAY';

            my $prev_row =
              ref $prev->{$key} eq 'ARRAY'
              ? $prev->{$key}->[-1]
              : $prev->{$key};

            my $merged = merge_rows([$prev_row, $row->{$key}]);
            if (@$merged > 1) {
                my $prev_rows =
                  ref $prev->{$key} eq 'ARRAY'
                  ? $prev->{$key}
                  : [$prev->{$key}];
                pop @$prev_rows;
                $prev->{$key} = [@$prev_rows, @$merged];
            }
        }
    }

    return $merged;
}

sub filter_columns {
    my ($meta_columns, $params) = @_;

    my $columns = $params->{columns} || $meta_columns;
    $columns = [$columns] unless ref $columns eq 'ARRAY';

    push @$columns, @{$params->{'+columns'}} if $params->{'+columns'};
    if ($params->{'-columns'}) {
        my $minus_columns = {map { $_ => 1 } @{$params->{'-columns'}}};
        $columns =
          [grep { !exists $minus_columns->{ref($_) ? $_->{'-col'} : $_} }
              @$columns];
    }

    return $columns;
}

1;
