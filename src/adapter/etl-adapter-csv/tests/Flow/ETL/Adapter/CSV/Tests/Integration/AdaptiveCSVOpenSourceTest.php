<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\AdaptiveCSVOpenSource;
use Flow\ETL\Adapter\CSV\CSVReadOptions;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Adapter\CSV\Tests\Double\LengthCapturingFilesystem;
use Flow\ETL\RustIterator;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Tests\Double\ThrowingSourceFilesystem;
use Generator;

use function extension_loaded;
use function iterator_to_array;

final class AdaptiveCSVOpenSourceTest extends FlowTestCase
{
    public function test_a_pinned_separator_wins_over_detection(): void
    {
        $open = new AdaptiveCSVOpenSource(
            new CountingFilesystem(new NativeLocalFilesystem()),
            CSVFixtureContext::source('semicolon.csv'),
            new CSVReadOptions(separator: ','),
        );

        try {
            static::assertSame(['id;name'], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_every_instance_opens_its_own_stream(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $source = CSVFixtureContext::source('header_only.csv');

        $first = new AdaptiveCSVOpenSource($filesystem, $source, new CSVReadOptions());
        $second = new AdaptiveCSVOpenSource($filesystem, $source, new CSVReadOptions());

        try {
            static::assertSame(['id', 'name'], $first->columns());
            static::assertSame(['id', 'name'], $second->columns());
        } finally {
            $first->close();
            $second->close();
        }
    }

    public function test_the_stream_is_closed_when_detection_throws(): void
    {
        $throwing = new ThrowingSourceFilesystem(new NativeLocalFilesystem());

        $this->expectException(RuntimeException::class);

        try {
            new AdaptiveCSVOpenSource($throwing, CSVFixtureContext::source('two_rows.csv'), new CSVReadOptions());
        } finally {
            static::assertTrue(
                $throwing->lastStream?->closed,
                'the constructor closes the stream it opened before rethrowing',
            );
        }
    }

    public function test_the_dialect_is_detected(): void
    {
        $open = new AdaptiveCSVOpenSource(
            new CountingFilesystem(new NativeLocalFilesystem()),
            CSVFixtureContext::source('semicolon.csv'),
            new CSVReadOptions(),
        );

        try {
            static::assertSame(['id', 'name'], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_the_source_is_opened_exactly_once(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $open = new AdaptiveCSVOpenSource($counting, CSVFixtureContext::source('two_rows.csv'), new CSVReadOptions());

        static::assertSame(1, $counting->readFromCalls);
        static::assertSame(0, $counting->closedStreams());

        $open->close();
    }

    public function test_with_the_extension_the_rust_source_reads(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $open = new AdaptiveCSVOpenSource(
            CSVFixtureContext::memory("id,name\n1,a\n"),
            CSVFixtureContext::memorySource(),
            new CSVReadOptions(),
        );

        try {
            static::assertInstanceOf(RustIterator::class, $open->records());
        } finally {
            $open->close();
        }
    }

    public function test_without_the_extension_the_php_source_reads(): void
    {
        if (extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is loaded');
        }

        $open = new AdaptiveCSVOpenSource(
            CSVFixtureContext::memory("id,name\n1,a\n"),
            CSVFixtureContext::memorySource(),
            new CSVReadOptions(),
        );

        try {
            static::assertInstanceOf(Generator::class, $open->records());
        } finally {
            $open->close();
        }
    }

    public function test_characters_read_in_line_reaches_the_opened_source(): void
    {
        $filesystem = new LengthCapturingFilesystem(CSVFixtureContext::memory("id,name\n1,a\n"));
        $open = new AdaptiveCSVOpenSource(
            $filesystem,
            CSVFixtureContext::memorySource(),
            (new CSVReadOptions())->withCharactersReadInLine(4096),
        );

        try {
            iterator_to_array($open->records());
        } finally {
            $open->close();
        }

        extension_loaded('flow_php')
            ? static::assertSame([4096], $filesystem->lastStream?->capturedIterateLengths)
            : static::assertSame([null, 4096], $filesystem->lastStream?->capturedLengths);
    }
}
