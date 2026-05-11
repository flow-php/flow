<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\to_memory;

final class AddDynamicEntriesTest extends FlowTestCase
{
    public function test_adding_new_entries(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
            ]))
            ->withEntry('updated_at', lit(new \DateTimeImmutable('2020-01-01 00:00:00 UTC')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertEquals(
            [
                ['id' => 1, 'updated_at' => new \DateTimeImmutable('2020-01-01T00:00:00+00:00')],
                ['id' => 2, 'updated_at' => new \DateTimeImmutable('2020-01-01T00:00:00+00:00')],
            ],
            $memory->dump(),
        );
    }
}
