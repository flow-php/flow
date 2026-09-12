<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_get;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\to_memory;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_structure;

final class ArrayGetTest extends FlowTestCase
{
    public function test_array_get(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                ['id' => 2],
            ]))
            ->withEntry('result', optional(ref('array')->arrayGet('b')))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'result' => 2],
                ['id' => 2, 'result' => null],
            ],
            $memory->dump(),
        );
    }

    public function test_nullsafe_path_reads_an_optional_collection(): void
    {
        data_frame()
            ->read(from_array(
                [['id' => 1, 'b' => ['x' => [1, 2]]], ['id' => 2, 'b' => []]],
                schema(
                    int_schema('id'),
                    structure_schema('b', type_structure([
                        'x' => structure_element('x', type_list(type_integer()), optional: true),
                    ])),
                ),
            ))
            ->withEntry('v', structure_get(ref('b'), '?x'))
            ->drop('b')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame([['id' => 1, 'v' => [1, 2]], ['id' => 2, 'v' => null]], $memory->dump());
    }
}
