<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\CountsByGroup;
use Flow\ETL\Functions\PerGroupAggregation;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Schema\Definitions;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_agg;
use function Flow\Types\DSL\type_string;

final class PerGroupAggregationTest extends FlowTestCase
{
    public function test_each_group_aggregates_its_own_rows_and_an_unseen_group_reads_the_fresh_value(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('k'), str_schema('note'));
        $rows = array_to_rows([
            ['k' => 'a', 'note' => 'x'],
            ['k' => 'b', 'note' => 'y'],
            ['k' => 'a', 'note' => 'z'],
        ], $schema);
        $aggregation = new PerGroupAggregation(
            string_agg(ref('note'), ','),
            Definitions::of('note_str_agg', type_string(), true),
        );

        $aggregation->aggregateGroups($rows, $backend->groupTable(['k'], $schema)->assign($rows), $backend);

        static::assertSame(['x,z', 'y', ''], $aggregation->groupValues(3, $backend)->values());
    }

    public function test_partials_are_each_groups_partial_and_an_unseen_group_reads_the_fresh_one(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('k'), str_schema('note'));
        $rows = array_to_rows([
            ['k' => 'a', 'note' => 'x'],
            ['k' => 'b', 'note' => 'y'],
            ['k' => 'a', 'note' => 'z'],
        ], $schema);
        $aggregation = new PerGroupAggregation(
            string_agg(ref('note'), ','),
            Definitions::of('note_str_agg', type_string(), true),
        );

        $aggregation->aggregateGroups($rows, $backend->groupTable(['k'], $schema)->assign($rows), $backend);

        static::assertSame(
            [['values' => ['x', 'z']], ['values' => ['y']], ['values' => []]],
            $aggregation->partials(3),
        );
    }

    public function test_absorbing_earlier_groups_puts_their_rows_in_front(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('k'), str_schema('note'));
        $definition = Definitions::of('note_str_agg', type_string(), true);
        $earlier = new PerGroupAggregation(string_agg(ref('note'), ','), $definition);
        $later = new PerGroupAggregation(string_agg(ref('note'), ','), $definition);
        $first = array_to_rows([['k' => 'a', 'note' => 'e1'], ['k' => 'b', 'note' => 'e2']], $schema);
        $second = array_to_rows([['k' => 'b', 'note' => 'l1']], $schema);
        $earlier->aggregateGroups($first, $backend->groupTable(['k'], $schema)->assign($first), $backend);
        $later->aggregateGroups($second, $backend->groupTable(['k'], $schema)->assign($second), $backend);

        $later->absorbEarlier($earlier, [0 => 1, 1 => 0], $backend);

        static::assertSame(['e2,l1', 'e1'], $later->groupValues(2, $backend)->values());
    }

    public function test_it_absorbs_only_another_per_group_aggregation(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new PerGroupAggregation(string_agg(ref('note')), Definitions::of('n', type_string(), true)))->absorbEarlier(
            new CountsByGroup(null, (new PhpBackend())->groupedCounts()),
            [],
            new PhpBackend(),
        );
    }
}
