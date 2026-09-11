<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Unit;

use Flow\Filesystem\Bridge\Azure\AzureBlobSourceStream;
use Flow\Filesystem\Bridge\Azure\Tests\Double\InMemoryBlobService;
use Flow\Filesystem\Tests\Context\ReadLinesContext;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class AzureBlobSourceStreamTest extends TestCase
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
        static::assertSame(
            $expected,
            iterator_to_array((new AzureBlobSourceStream(
                path('azure-blob://f.txt'),
                new InMemoryBlobService($content),
            ))->readLines($separator, $length)),
        );
    }

    public function test_read_lines_stops_when_a_read_returns_no_bytes(): void
    {
        static::assertSame(
            [],
            iterator_to_array(
                (new AzureBlobSourceStream(path('azure-blob://f.txt'), new InMemoryBlobService('', 5, 3)))->readLines(),
            ),
        );
    }
}
