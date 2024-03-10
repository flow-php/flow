<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_sequence_number, lit, ref, to_memory, when};
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class WhenTest extends TestCase
{
    public function test_when_odd_even() : void
    {
        df()
            ->read(from_sequence_number('id', 1, 10))
            ->collect()
            ->withEntry(
                'type',
                when(
                    ref('id')->isOdd(),
                    lit('odd'),
                    lit('even')
                )
            )
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'type' => 'odd'],
                ['id' => 2, 'type' => 'even'],
                ['id' => 3, 'type' => 'odd'],
                ['id' => 4, 'type' => 'even'],
                ['id' => 5, 'type' => 'odd'],
                ['id' => 6, 'type' => 'even'],
                ['id' => 7, 'type' => 'odd'],
                ['id' => 8, 'type' => 'even'],
                ['id' => 9, 'type' => 'odd'],
                ['id' => 10, 'type' => 'even'],
            ],
            $memory->dump()
        );
    }
}
