<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{from_array, lit, to_memory};
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class AddDynamicEntriesTest extends TestCase
{
    public function test_adding_new_entries() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                        ['id' => 2],
                    ]
                )
            )
            ->withEntry('updated_at', lit(new \DateTimeImmutable('2020-01-01 00:00:00 UTC')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertEquals(
            [
                ['id' => 1, 'updated_at' => new \DateTimeImmutable('2020-01-01T00:00:00+00:00')],
                ['id' => 2, 'updated_at' => new \DateTimeImmutable('2020-01-01T00:00:00+00:00')],
            ],
            $memory->dump()
        );
    }
}
