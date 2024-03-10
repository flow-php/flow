<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class CastTest extends TestCase
{
    public function test_cast() : void
    {
        df()
            ->read(from_array(
                [
                    ['date' => new \DateTimeImmutable('2023-01-01')],
                ]
            ))
            ->withEntry('date', ref('date')->cast('string'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertEquals(
            [
                ['date' => '2023-01-01T00:00:00+00:00'],
            ],
            $memory->dump()
        );
    }
}
