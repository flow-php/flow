<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\when;

final class NotTest extends FlowTestCase
{
    public function test_not(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
            ]))
            ->withEntry('result', when(not(ref('array')->isNotNull()), lit('not found'), lit('found')))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            // A ragged source now yields one column set, so `array` exists on every row and
            // `exists()` no longer discriminates - `isNotNull()` is what distinguishes the rows.
            [
                ['id' => 1, 'result' => 'found'],
                ['id' => 2, 'result' => 'not found'],
                ['id' => 3, 'result' => 'not found'],
                ['id' => 4, 'result' => 'found'],
            ],
            $memory->dump(),
        );
    }
}
