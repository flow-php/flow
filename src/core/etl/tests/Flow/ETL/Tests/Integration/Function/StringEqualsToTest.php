<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class StringEqualsToTest extends FlowTestCase
{
    public function test_equals_to(): void
    {
        data_frame()
            ->read(from_array([
                ['text' => 'hello', 'compare' => 'hello'],
                ['text' => 'hello', 'compare' => 'world'],
                ['text' => 'hello', 'compare' => 'Hello'],
                ['text' => '', 'compare' => ''],
                ['text' => 'hello', 'compare' => ''],
                ['text' => 'नमस्ते', 'compare' => 'नमस्ते'],
                ['text' => 'नमस्ते', 'compare' => 'नमस्कार'],
                ['text' => null, 'compare' => 'hello'],
                ['text' => 'hello', 'compare' => null],
                ['text' => '🚀🌟', 'compare' => '🚀🌟'],
            ]))
            ->withEntry('equals', optional(ref('text')->stringEqualsTo(ref('compare'))))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['text' => 'hello', 'compare' => 'hello', 'equals' => true],
                ['text' => 'hello', 'compare' => 'world', 'equals' => false],
                ['text' => 'hello', 'compare' => 'Hello', 'equals' => false],
                ['text' => '', 'compare' => '', 'equals' => true],
                ['text' => 'hello', 'compare' => '', 'equals' => false],
                ['text' => 'नमस्ते', 'compare' => 'नमस्ते', 'equals' => true],
                ['text' => 'नमस्ते', 'compare' => 'नमस्कार', 'equals' => false],
                ['text' => null, 'compare' => 'hello', 'equals' => null],
                ['text' => 'hello', 'compare' => null, 'equals' => null],
                ['text' => '🚀🌟', 'compare' => '🚀🌟', 'equals' => true],
            ],
            $memory->dump(),
        );
    }
}
