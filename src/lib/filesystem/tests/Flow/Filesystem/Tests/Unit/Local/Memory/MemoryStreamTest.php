<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Local\Memory;

use Flow\Filesystem\Exception\FileExistsException;
use Flow\Filesystem\Local\Memory\Memory;
use Flow\Filesystem\Tests\Context\ReadLinesContext;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class MemoryStreamTest extends TestCase
{
    public function test_exclusive_stream_closes_once(): void
    {
        $memory = new Memory();
        $memory->for(path('memory://file.txt'))->append('first');
        $stream = $memory->exclusive(path('memory://file.txt'))->append('second');

        try {
            $stream->close();
            static::fail('Expected the existing path to be refused');
        } catch (FileExistsException) {
        }

        $stream->close();

        static::assertSame('first', $memory->for(path('memory://file.txt'))->content());
    }

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
