<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Column\Php;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\Php\PhpGroupedCounts;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PhpGroupedCountsTest extends FlowTestCase
{
    public function test_it_counts_every_non_null_value_of_any_type_into_its_group_across_batches(): void
    {
        $counts = new PhpGroupedCounts();
        $schema = schema(str_schema('v', nullable: true), int_schema('id'));
        $first = array_to_rows([['v' => 'a', 'id' => 0], ['v' => null, 'id' => 1], ['v' => 'x', 'id' => 0]], $schema);
        $second = array_to_rows([['v' => 'b', 'id' => 2], ['v' => 'c', 'id' => 1]], $schema);

        $counts->add($first->column('v'), $first->column('id'));
        $counts->add($second->column('v'), $second->column('id'));

        static::assertSame([2, 1, 1], $counts->counts()->values());
        static::assertSame([2, 1, 1], $counts->partials());
    }

    public function test_a_group_holding_only_nulls_counts_zero(): void
    {
        $counts = new PhpGroupedCounts();
        $rows = array_to_rows([['v' => null, 'id' => 0]], schema(str_schema('v', nullable: true), int_schema('id')));

        $counts->add($rows->column('v'), $rows->column('id'));

        static::assertSame([0], $counts->counts()->values());
    }

    public function test_absorbing_earlier_counts_adds_them_into_the_mapped_groups(): void
    {
        $schema = schema(str_schema('v'), int_schema('id'));
        $earlier = new PhpGroupedCounts();
        $rows = array_to_rows([['v' => 'a', 'id' => 0], ['v' => 'b', 'id' => 1], ['v' => 'c', 'id' => 1]], $schema);
        $earlier->add($rows->column('v'), $rows->column('id'));
        $later = new PhpGroupedCounts();
        $rows = array_to_rows([['v' => 'x', 'id' => 0], ['v' => 'y', 'id' => 1]], $schema);
        $later->add($rows->column('v'), $rows->column('id'));

        $later->absorbEarlier($earlier, [0 => 1, 1 => 2]);

        static::assertSame([1, 2, 2], $later->partials());
    }

    public function test_absorbing_an_earlier_group_it_never_counted_fails(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Earlier group 3 has no count');

        (new PhpGroupedCounts())->absorbEarlier(new PhpGroupedCounts(), [3 => 0]);
    }
}
