<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Row\Reference\Expression;

use function Flow\ETL\DSL\array_get;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class ArrayGetTest extends TestCase
{
    public function test_array_get() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                        ['id' => 2],
                    ]
                )
            )
            ->withEntry('row', ref('row')->unpack())
            ->renameAll('row.', '')
            ->withEntry('result', array_get(ref('array'), 'b'))
            ->drop('row', 'array')
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'result' => 2],
                ['id' => 2, 'result' => null],
            ],
            $memory->data
        );
    }
}
