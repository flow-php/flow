<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function\Structure;

use Flow\ETL\DataFrame;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class StructureTest extends FlowTestCase
{
    public function test_structure_column_from_flat_columns(): void
    {
        $frame = static fn(): DataFrame => df()
            ->read(from_array(
                [['line_id' => 'li-1', 'quantity' => 2], ['line_id' => 'li-2', 'quantity' => null]],
                schema(str_schema('line_id'), int_schema('quantity', nullable: true)),
            ))
            ->withEntry('line_item', structure(['id' => ref('line_id'), 'quantity' => ref('quantity')]));

        static::assertEquals(
            schema(
                str_schema('line_id'),
                int_schema('quantity', nullable: true),
                structure_schema('line_item', type_structure([
                    'id' => type_string(),
                    'quantity' => type_optional(type_integer()),
                ])),
            ),
            $frame()->schema(),
        );
        static::assertSame(
            [
                ['line_id' => 'li-1', 'quantity' => 2, 'line_item' => ['id' => 'li-1', 'quantity' => 2]],
                ['line_id' => 'li-2', 'quantity' => null, 'line_item' => ['id' => 'li-2', 'quantity' => null]],
            ],
            $frame()->fetch()->toArray(),
        );
    }

    public function test_structure_with_numeric_keys(): void
    {
        $frame = static fn(): DataFrame => df()
            ->read(from_array([['id' => 1, 'city' => 'Krakow']], schema(int_schema('id'), str_schema('city'))))
            ->withEntry('user', structure([5 => ref('id'), 7 => ref('city')]));

        static::assertSame('structure{5: integer, 7: string}', $frame()->schema()->get('user')->type()->toString());
        static::assertSame(
            [['id' => 1, 'city' => 'Krakow', 'user' => [5 => 1, 7 => 'Krakow']]],
            $frame()->fetch()->toArray(),
        );
    }

    public function test_nested_structure(): void
    {
        $frame = static fn(): DataFrame => df()
            ->read(from_array([['id' => 1, 'city' => 'Krakow']], schema(int_schema('id'), str_schema('city'))))
            ->withEntry('user', structure(['id' => ref('id'), 'address' => structure(['city' => ref('city')])]));

        static::assertSame(
            'structure{id: integer, address: structure{city: string}}',
            $frame()->schema()->get('user')->type()->toString(),
        );
        static::assertSame(
            [['id' => 1, 'city' => 'Krakow', 'user' => ['id' => 1, 'address' => ['city' => 'Krakow']]]],
            $frame()->fetch()->toArray(),
        );
    }
}
