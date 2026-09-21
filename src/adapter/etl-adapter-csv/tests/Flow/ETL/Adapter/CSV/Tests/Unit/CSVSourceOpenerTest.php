<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVReadOptions;
use Flow\ETL\Adapter\CSV\CSVSourceOpener;
use Flow\ETL\Adapter\CSV\NativeCSVOpenSource;
use Flow\ETL\Adapter\CSV\PhpCSVOpenSource;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Adapter\CSV\Tests\Double\LengthCapturingFilesystem;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Tests\Double\ThrowingSourceFilesystem;
use PHPUnit\Framework\TestCase;

use function iterator_to_array;

final class CSVSourceOpenerTest extends TestCase
{
    public function test_open_returns_the_native_source_when_the_extension_is_loaded(): void
    {
        if (!NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is not loaded');
        }

        $open = (new CSVSourceOpener(CSVFixtureContext::memory("id,name\n1,a\n"), new CSVReadOptions()))->open(
            CSVFixtureContext::memorySource(),
        );

        try {
            static::assertInstanceOf(NativeCSVOpenSource::class, $open);
            static::assertSame(['id', 'name'], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_open_returns_the_php_source_when_the_extension_is_absent(): void
    {
        if (NativeCSVOpenSource::isSupported()) {
            static::markTestSkipped('flow_php extension with RustCSVReaderNative is loaded');
        }

        $open = (new CSVSourceOpener(CSVFixtureContext::memory("id,name\n1,a\n"), new CSVReadOptions()))->open(
            CSVFixtureContext::memorySource(),
        );

        try {
            static::assertInstanceOf(PhpCSVOpenSource::class, $open);
            static::assertSame(['id', 'name'], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_a_failing_detection_closes_the_stream_on_both_paths(): void
    {
        $throwing = new ThrowingSourceFilesystem(CSVFixtureContext::memory("id,name\n1,a\n"));

        $this->expectException(RuntimeException::class);

        try {
            (new CSVSourceOpener($throwing, new CSVReadOptions()))->open(CSVFixtureContext::memorySource());
        } finally {
            static::assertTrue($throwing->lastStream?->closed, 'open() closes the stream it opened before rethrowing');
        }
    }

    public function test_characters_read_in_line_reaches_the_opened_source(): void
    {
        $filesystem = new LengthCapturingFilesystem(CSVFixtureContext::memory("id,name\n1,a\n"));
        $open = (new CSVSourceOpener($filesystem, (new CSVReadOptions())->withCharactersReadInLine(4096)))->open(
            CSVFixtureContext::memorySource(),
        );

        try {
            iterator_to_array($open->records());
        } finally {
            $open->close();
        }

        NativeCSVOpenSource::isSupported()
            ? static::assertSame([4096], $filesystem->lastStream?->capturedIterateLengths)
            : static::assertSame([null, 4096], $filesystem->lastStream?->capturedLengths);
    }
}
