<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Functions;

use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class PrependTest extends FlowTestCase
{
    public function test_prepend_basic_operation(): void
    {
        data_frame()
            ->read(from_array([
                ['name' => 'Doe'],
            ]))
            ->withColumn('greeting', ref('name')->prepend(lit('Mr. ')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['name' => 'Doe', 'greeting' => 'Mr. Doe'],
            ],
            $memory->dump(),
        );
    }
}
