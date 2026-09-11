<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Local\Memory;

use Flow\Filesystem\Tests\Context\ReadLinesContext;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class MemoryStreamTest extends TestCase
{
    /**
     * @param non-empty-string $separator
     * @param null|int<1, max> $length
     * @param list<string> $expected
     */
    #[DataProviderExternal(ReadLinesContext::class, 'cases')]
    public function test_read_lines_conforms_to_the_source_stream_contract(
        string $content,
        string $separator,
        ?int $length,
        array $expected,
    ): void {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://lines.txt'))->append($content)->close();

        static::assertSame(
            $expected,
            iterator_to_array($filesystem->readFrom(path('memory://lines.txt'))->readLines($separator, $length)),
        );
    }
}
