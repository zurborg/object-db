package ObjectDB::Related::OneToOne;

use strict;
use warnings;

use base 'ObjectDB::Related::ManyToOne';

use Scalar::Util ();

sub create_related {
    my $self = shift;
    my ($row) = shift;

    my $meta = $self->{meta};

    my ($from, $to) = %{$meta->map};

    my @params = ($to => $row->column($from));

    my @related =
      @_ == 1 ? ref $_[0] eq 'ARRAY' ? @{$_[0]} : ($_[0]) : ({@_});

    my @objects;
    foreach my $related (@related) {
        if (!Scalar::Util::blessed($related)) {
            $related = $meta->class->new(%$related);
        }
        $related->set_columns(@params);
        $related->create;
        push @objects, $related;
    }

    return @related == 1 ? $objects[0] : @objects;
}

sub update_related {
    my $self = shift;
    my ($row) = shift;

    my %params = @_ == 1 ? %{$_[0]} : @_;

    my $meta = $self->meta;

    my $rel_table = $meta->class->meta->table;

    my ($from, $to) = %{$meta->map};
    my $where = ["$rel_table.$to" => $row->get_column($from)];

    push @$where, @{$params{where}} if $params{where};

    return $meta->class->table->update(where => $where, @_);
}

sub delete_related {
    my $self = shift;
    my ($row) = shift;

    my %params = @_;
    $params{where} ||= [];

    my $meta = $self->meta;

    my ($from, $to) = %{$meta->map};

    push @{$params{where}}, ($to => $row->get_column($from));

    return $meta->class->table->delete(%params);
}

1;