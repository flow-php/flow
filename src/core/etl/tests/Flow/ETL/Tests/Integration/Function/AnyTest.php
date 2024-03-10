<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{any, from_array, lit, ref, to_memory, when};
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class AnyTest extends TestCase
{
    public function test_any_case_found() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                        ['id' => 2],
                        ['id' => 3],
                        ['id' => 4, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                    ]
                )
            )
            ->withEntry(
                'result',
                when(
                    any(ref('id')->isEven(), ref('array')->exists('b')),
                    lit('found'),
                    lit('not found')
                )
            )
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'result' => 'found'],
                ['id' => 2, 'result' => 'found'],
                ['id' => 3, 'result' => 'not found'],
                ['id' => 4, 'result' => 'found'],
            ],
            $memory->dump()
        );
    }
}
