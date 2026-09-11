<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rename_replace;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\to_memory;

final class ArrayUnpackTest extends FlowTestCase
{
    public function test_array_unpack(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                ['id' => 2, 'array' => []],
            ]))
            ->withEntry('array', ref('array')->unpack(schema(int_schema('a'), int_schema('b'), int_schema('c'))))
            ->renameEach(rename_replace('array.', ''))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        // every declared column is produced, so a payload that omits one carries null rather than
        // dropping the column
        static::assertSame(
            [
                ['id' => 1, 'a' => 1, 'b' => 2, 'c' => 3],
                ['id' => 2, 'a' => null, 'b' => null, 'c' => null],
            ],
            $memory->dump(),
        );
    }
}
