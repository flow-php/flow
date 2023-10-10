<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Row\Reference\Expression;

use function Flow\ETL\DSL\combine;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class CombineTest extends TestCase
{
    public function test_combine() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => 1, 'first' => ['a', 'b', 'c'], 'second' => [1, 2, 3]],
                        ['id' => 2],
                    ]
                )
            )
            ->withEntry('row', ref('row')->unpack())
            ->renameAll('row.', '')
            ->withEntry('array', combine(ref('first'), ref('second')))
            ->drop('row', 'first', 'second')
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                ['id' => 2, 'array' => null],
            ],
            $memory->data
        );
    }
}
