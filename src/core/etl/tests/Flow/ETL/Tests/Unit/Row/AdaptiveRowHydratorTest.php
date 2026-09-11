<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class AdaptiveRowHydratorTest extends FlowTestCase
{
    public function test_hydrate_builds_rows_from_values_against_a_schema(): void
    {
        $rows = (new AdaptiveRowHydrator())->hydrate(
            [new RawRowValues(['id' => 1, 'name' => 'flow']), new RawRowValues(['id' => 2, 'name' => null])],
            schema(int_schema('id'), str_schema('name', nullable: true)),
        );

        static::assertSame(2, $rows->count());
        static::assertSame(1, $rows->first()->get('id'));
        static::assertNull($rows->all()[1]->get('name'));
    }

    public function test_dehydrate_turns_rows_into_typed_values(): void
    {
        $dehydrated = (new AdaptiveRowHydrator())->dehydrate(rows(
            schema(int_schema('id'), str_schema('name')),
            row(['id' => 1, 'name' => 'flow']),
        ));

        static::assertCount(1, $dehydrated);
        static::assertSame(['id' => 1, 'name' => 'flow'], $dehydrated[0]->values);
    }

    public function test_hydrate_throws_on_missing_required_structure_element(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('Rows do not match their schema: column "data" (row 0)');

        (new AdaptiveRowHydrator())->hydrate([new RawRowValues(['data' => [
            'id' => 1,
        ]])], schema(structure_schema('data', type_structure(['id' => type_integer(), 'name' => type_string()]))));
    }
}
