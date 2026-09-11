<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Stream;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Filesystem\Tests\Context\ReadLinesContext;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

use function iterator_to_array;

final class MemorySourceStreamTest extends TestCase
{
    /**
     * @param non-empty-string $content
     * @param non-empty-string $separator
     * @param null|int<1, max> $length
     * @param list<string> $expected
     */
    #[DataProviderExternal(ReadLinesContext::class, 'non_empty_cases')]
    public function test_read_lines_conforms_to_the_source_stream_contract(
        string $content,
        string $separator,
        ?int $length,
        array $expected,
    ): void {
        static::assertSame(
            $expected,
            iterator_to_array((new MemorySourceStream($content))->readLines($separator, $length)),
        );
    }
}
