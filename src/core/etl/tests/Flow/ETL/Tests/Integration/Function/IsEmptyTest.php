<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class IsEmptyTest extends FlowTestCase
{
    public function test_is_empty() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => ''],
                        ['text' => 'hello'],
                        ['text' => ' '],
                        ['text' => "\t"],
                        ['text' => "\n"],
                        ['text' => 'café'],
                        ['text' => '🚀'],
                        ['text' => null],
                        ['text' => 'a'],
                        ['text' => '!@#$%'],
                    ]
                )
            )
            ->withEntry('is_empty', ref('text')->isEmpty())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => '', 'is_empty' => true],
                ['text' => 'hello', 'is_empty' => false],
                ['text' => ' ', 'is_empty' => false],
                ['text' => "\t", 'is_empty' => false],
                ['text' => "\n", 'is_empty' => false],
                ['text' => 'café', 'is_empty' => false],
                ['text' => '🚀', 'is_empty' => false],
                ['text' => null, 'is_empty' => null],
                ['text' => 'a', 'is_empty' => false],
                ['text' => '!@#$%', 'is_empty' => false],
            ],
            $memory->dump()
        );
    }
}
