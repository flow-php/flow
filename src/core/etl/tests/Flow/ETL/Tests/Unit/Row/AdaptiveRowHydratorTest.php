<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\str_schema;

final class AdaptiveRowHydratorTest extends FlowTestCase
{
    public function test_hydrate_builds_rows_from_values_against_a_schema(): void
    {
        $rows = (new AdaptiveRowHydrator())->hydrate(
            [new RawRowValues(['id' => 1, 'name' => 'flow']), new RawRowValues(['id' => 2, 'name' => null])],
            schema(int_schema('id'), str_schema('name', nullable: true)),
        );

        static::assertSame(2, $rows->count());
        static::assertSame(1, $rows->first()->valueOf('id'));
        static::assertNull($rows->all()[1]->valueOf('name'));
    }

    public function test_dehydrate_turns_rows_into_typed_values(): void
    {
        $dehydrated = (new AdaptiveRowHydrator())->dehydrate(rows(row(int_entry('id', 1), str_entry('name', 'flow'))));

        static::assertCount(1, $dehydrated);
        static::assertSame(['id' => 1, 'name' => 'flow'], $dehydrated[0]->values);
    }

    public function test_cast_infers_rows_without_a_schema(): void
    {
        $rows = (new AdaptiveRowHydrator())->cast([new RawRowValues(['id' => 1, 'name' => 'x'])]);

        static::assertSame(1, $rows->first()->valueOf('id'));
        static::assertSame('x', $rows->first()->valueOf('name'));
    }
}
