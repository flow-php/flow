<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class BetweenTest extends TestCase
{
    public function test_between_comparisons() : void
    {
        df()
            ->read(
                from_array(
                    [
                        ['val' => new \DateTimeImmutable('2023-01-10 00:00:00')],
                    ]
                )
            )
            ->withEntry('between', ref('val')->between(lit(new \DateTimeImmutable('2023-01-01 00:00:00')), lit(new \DateTimeImmutable('2023-01-20 00:00:00'))))
            ->withEntry('not_between', ref('val')->between(lit(new \DateTimeImmutable('2023-01-01 00:00:00')), lit(new \DateTimeImmutable('2023-01-08 00:00:00'))))
            ->select('between', 'not_between')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                [
                    'between' => true,
                    'not_between' => false,
                ],
            ],
            $memory->dump()
        );
    }
}
