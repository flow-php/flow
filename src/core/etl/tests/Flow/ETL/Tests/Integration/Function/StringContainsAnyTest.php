<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class StringContainsAnyTest extends FlowTestCase
{
    public function test_contains_any() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world', 'needles' => ['hello', 'foo']],
                        ['text' => 'hello world', 'needles' => ['foo', 'bar']],
                        ['text' => 'hello world', 'needles' => ['world', 'test']],
                        ['text' => 'hello world', 'needles' => []],
                        ['text' => '', 'needles' => ['hello']],
                        ['text' => null, 'needles' => ['hello']],
                        ['text' => 'hello world', 'needles' => null],
                        ['text' => 'नमस्ते', 'needles' => ['स्ते', 'foo']],
                        ['text' => 'hello🚀world', 'needles' => ['🚀', 'bar']],
                        ['text' => 'testing', 'needles' => ['test', 'ing']],
                    ]
                )
            )
            ->withEntry('contains_any', ref('text')->stringContainsAny(ref('needles')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'needles' => ['hello', 'foo'], 'contains_any' => true],
                ['text' => 'hello world', 'needles' => ['foo', 'bar'], 'contains_any' => false],
                ['text' => 'hello world', 'needles' => ['world', 'test'], 'contains_any' => true],
                ['text' => 'hello world', 'needles' => [], 'contains_any' => false],
                ['text' => '', 'needles' => ['hello'], 'contains_any' => false],
                ['text' => null, 'needles' => ['hello'], 'contains_any' => false],
                ['text' => 'hello world', 'needles' => null, 'contains_any' => false],
                ['text' => 'नमस्ते', 'needles' => ['स्ते', 'foo'], 'contains_any' => true],
                ['text' => 'hello🚀world', 'needles' => ['🚀', 'bar'], 'contains_any' => true],
                ['text' => 'testing', 'needles' => ['test', 'ing'], 'contains_any' => true],
            ],
            $memory->dump()
        );
    }
}
