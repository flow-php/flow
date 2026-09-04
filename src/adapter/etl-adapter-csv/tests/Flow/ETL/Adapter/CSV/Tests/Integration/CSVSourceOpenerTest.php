<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\CSVReadOptions;
use Flow\ETL\Adapter\CSV\CSVSourceOpener;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Tests\Double\ThrowingSourceFilesystem;

final class CSVSourceOpenerTest extends FlowTestCase
{
    public function test_a_pinned_separator_wins_over_detection(): void
    {
        $open = (new CSVSourceOpener(
            new CountingFilesystem(new NativeLocalFilesystem()),
            new CSVReadOptions(separator: ','),
        ))->open(CSVFixtureContext::source('semicolon.csv'));

        try {
            static::assertSame(',', $open->dialect->separator);
            static::assertSame(['id;name'], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_open_builds_a_fresh_encoder_per_call(): void
    {
        $opener = new CSVSourceOpener(new CountingFilesystem(new NativeLocalFilesystem()), new CSVReadOptions());
        $source = CSVFixtureContext::source('header_only.csv');

        $first = $opener->open($source);
        $second = $opener->open($source);

        try {
            static::assertSame(['id', 'name'], $first->columns());
            static::assertSame(['id', 'name'], $second->columns());
        } finally {
            $first->close();
            $second->close();
        }
    }

    public function test_open_closes_the_stream_when_detection_throws(): void
    {
        $throwing = new ThrowingSourceFilesystem(new NativeLocalFilesystem());
        $opener = new CSVSourceOpener($throwing, new CSVReadOptions());

        $this->expectException(RuntimeException::class);

        try {
            $opener->open(CSVFixtureContext::source('two_rows.csv'));
        } finally {
            static::assertTrue($throwing->lastStream?->closed, 'open() closes the stream it opened before rethrowing');
        }
    }

    public function test_open_detects_the_dialect(): void
    {
        $open = (new CSVSourceOpener(
            new CountingFilesystem(new NativeLocalFilesystem()),
            new CSVReadOptions(),
        ))->open(CSVFixtureContext::source('semicolon.csv'));

        try {
            static::assertSame(';', $open->dialect->separator);
        } finally {
            $open->close();
        }
    }

    public function test_open_opens_the_source_exactly_once(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $open = (new CSVSourceOpener($counting, new CSVReadOptions()))->open(CSVFixtureContext::source('two_rows.csv'));

        static::assertSame(1, $counting->readFromCalls);
        static::assertSame(0, $counting->closedStreams());

        $open->close();
    }
}
