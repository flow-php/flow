<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Row\Reference\Expression;

use function Flow\ETL\DSL\array_merge;
use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class ArrayMergeTest extends TestCase
{
    public function test_array_merge() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => 1, 'first' => ['a' => 1, 'b' => 2], 'second' => ['c' => 3]],
                        ['id' => 2],
                    ]
                )
            )
            ->withEntry('array', optional(array_merge(ref('first'), ref('second'))))
            ->drop('first', 'second')
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
