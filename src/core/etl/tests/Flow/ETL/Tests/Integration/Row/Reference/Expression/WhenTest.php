<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Row\Reference\Expression;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\when;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class WhenTest extends TestCase
{
    public function test_when_odd_even() : void
    {
        (new Flow())
            ->read(From::sequence_number('id', 1, 10))
            ->collect()
            ->withEntry(
                'type',
                when(
                    ref('id')->isOdd(),
                    lit('odd'),
                    lit('even')
                )
            )
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
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
            $memory->data
        );
    }
}
