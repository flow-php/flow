<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration\IO\Extractor;

use Flow\ETL\Adapter\CSV\IO\CSVDialect;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVDialects;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVReadOptions;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVSourceOpener;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Adapter\CSV\Tests\Double\LengthCapturingFilesystem;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\RuntimeException as ETLRuntimeException;
use Flow\ETL\Foundation\FlowPhpExtension;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\InferredColumns;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\RustIterator;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Tests\Double\ThrowingSourceFilesystem;
use Flow\Filesystem\Tests\Double\ThrowingSourceStream;
use Generator;
use PHPUnit\Framework\Attributes\TestWith;

use function extension_loaded;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_array;
use function iterator_to_array;

final class CSVSourceOpenerTest extends FlowTestCase
{
    public function test_a_pinned_separator_wins_over_detection(): void
    {
        $open = (new CSVSourceOpener(
            new CountingFilesystem(new NativeLocalFilesystem()),
            new CSVReadOptions(separator: ','),
        ))->open(CSVFixtureContext::source('semicolon.csv'));

        try {
            static::assertSame(['id;name'], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_a_fully_declared_dialect_is_not_detected(): void
    {
        $dialect = (new CSVSourceOpener(
            new NativeLocalFilesystem(),
            new CSVReadOptions(separator: ';', enclosure: '"', escape: '\\'),
        ))->dialect(new ThrowingSourceStream(path(CSVFixtureContext::path('semicolon.csv'))));

        static::assertSame([';', '"', '\\'], [$dialect->separator, $dialect->enclosure, $dialect->escape]);
    }

    #[TestWith([0, 12, true])]
    #[TestWith([13, 8, true])]
    #[TestWith([0, 12, false])]
    #[TestWith([13, 8, false])]
    public function test_a_split_opens_its_file_once(int $start, int $length, bool $native): void
    {
        if ($native && !FlowPhpExtension::detect()->available()) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $filesystem = new LengthCapturingFilesystem(new NativeLocalFilesystem());
        static::assertNotSame(
            [],
            CSVFixtureContext::records((new CSVSourceOpener(
                $filesystem,
                new CSVReadOptions(),
                extension: $native ? null : new FlowPhpExtension(false, null),
            ))->openSplit(
                CSVFixtureContext::source('five_rows.csv'),
                new FileSplit(path(CSVFixtureContext::path('five_rows.csv')), $start, $length),
            )),
        );
        static::assertSame(1, $filesystem->opens());
    }

    public function test_only_the_first_window_refuses_a_diverging_header(): void
    {
        $opener = new CSVSourceOpener(new NativeLocalFilesystem(), new CSVReadOptions());
        $source = CSVFixtureContext::source('five_rows.csv');
        $inferred = new InferredColumns(
            schema(str_schema('id'), str_schema('title')),
            [],
            new SchemaInference(),
            'other.csv',
        );

        static::assertSame(
            [['id' => '3', 'name' => 'c'], ['id' => '4', 'name' => 'd']],
            CSVFixtureContext::records($opener->openSplit($source, new FileSplit($source->path, 13, 8), $inferred)),
        );

        $this->expectException(InferredSchemaException::class);

        $opener->openSplit($source, new FileSplit($source->path, 0, 12), $inferred);
    }

    public function test_columns_leaves_the_stream_open(): void
    {
        $opener = new CSVSourceOpener(new NativeLocalFilesystem(), new CSVReadOptions());
        $stream = (new NativeLocalFilesystem())->readFrom(path(CSVFixtureContext::path('five_rows.csv')));

        try {
            static::assertSame(['id', 'name'], $opener->columns($stream, $opener->dialect($stream)));
            static::assertTrue($stream->isOpen());
        } finally {
            $stream->close();
        }

        static::assertSame(
            [['id' => '3', 'name' => 'c'], ['id' => '4', 'name' => 'd']],
            CSVFixtureContext::records($opener->openSplit(
                CSVFixtureContext::source('five_rows.csv'),
                new FileSplit(path(CSVFixtureContext::path('five_rows.csv')), 13, 8),
            )),
        );
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

    public function test_a_known_dialect_is_used_and_nothing_is_detected(): void
    {
        $source = CSVFixtureContext::source('semicolon.csv');
        $dialects = new CSVDialects();
        $dialects->learn($source->uri(), new CSVDialect(',', '"', '\\'));
        $opener = new CSVSourceOpener(new NativeLocalFilesystem(), new CSVReadOptions(), $dialects);
        $open = $opener->open($source);
        $split = $opener->openSplit($source, new FileSplit($source->path, 0, null));

        try {
            static::assertSame(['id;name'], $open->columns());
            static::assertSame(['id;name'], $split->columns());
        } finally {
            $open->close();
            $split->close();
        }
    }

    public function test_a_detected_dialect_is_learned(): void
    {
        $source = CSVFixtureContext::source('semicolon.csv');
        $dialects = new CSVDialects();
        (new CSVSourceOpener(new NativeLocalFilesystem(), new CSVReadOptions(), $dialects))
            ->open($source)
            ->close();

        static::assertEquals(new CSVDialect(';', '"', '\\'), $dialects->for($source->uri()));
    }

    public function test_open_detects_the_dialect(): void
    {
        $open = (new CSVSourceOpener(
            new CountingFilesystem(new NativeLocalFilesystem()),
            new CSVReadOptions(),
        ))->open(CSVFixtureContext::source('semicolon.csv'));

        try {
            static::assertSame(['id', 'name'], $open->columns());
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

    public function test_open_split_closes_the_stream_when_detection_throws(): void
    {
        $throwing = new ThrowingSourceFilesystem(new NativeLocalFilesystem());
        $opener = new CSVSourceOpener($throwing, new CSVReadOptions());

        $this->expectException(RuntimeException::class);

        try {
            $opener->openSplit(
                CSVFixtureContext::source('two_rows.csv'),
                new FileSplit(path(CSVFixtureContext::path('two_rows.csv')), 0, null),
            );
        } finally {
            static::assertTrue(
                $throwing->lastStream?->closed,
                'openSplit() closes the stream it opened before rethrowing',
            );
        }
    }

    public function test_open_split_of_a_file_without_columns_reads_nothing(): void
    {
        static::assertSame(
            [],
            CSVFixtureContext::records((new CSVSourceOpener(
                new NativeLocalFilesystem(),
                new CSVReadOptions(),
            ))->openSplit(
                CSVFixtureContext::source('empty.csv'),
                new FileSplit(path(CSVFixtureContext::path('empty.csv')), 0, 10),
            )),
        );
    }

    public function test_open_split_reads_the_first_window_with_its_header(): void
    {
        static::assertSame(
            [['id' => '1', 'name' => 'a']],
            CSVFixtureContext::records((new CSVSourceOpener(
                new NativeLocalFilesystem(),
                new CSVReadOptions(),
            ))->openSplit(
                CSVFixtureContext::source('five_rows.csv'),
                new FileSplit(path(CSVFixtureContext::path('five_rows.csv')), 0, 12),
            )),
        );
    }

    public function test_open_split_reads_only_the_records_starting_in_its_window(): void
    {
        static::assertSame(
            [['id' => '3', 'name' => 'c'], ['id' => '4', 'name' => 'd']],
            CSVFixtureContext::records((new CSVSourceOpener(
                new NativeLocalFilesystem(),
                new CSVReadOptions(),
            ))->openSplit(
                CSVFixtureContext::source('five_rows.csv'),
                new FileSplit(path(CSVFixtureContext::path('five_rows.csv')), 13, 8),
            )),
        );
    }

    public function test_an_opener_over_the_local_filesystem_describes_and_rebuilds_equal(): void
    {
        $opener = new CSVSourceOpener(new NativeLocalFilesystem(), (new CSVReadOptions())->withSeparator(';'));
        $description = $opener->describe();

        static::assertNotNull($description);
        static::assertTrue($description->sensitive());
        static::assertSame(';', type_array()->assert($description->data['options'])['separator']);
        static::assertEquals($opener, (new Rebuild())->object(
            Description::fromArray($description->normalize()),
            CSVSourceOpener::class,
        ));
    }

    public function test_an_opener_over_a_memory_filesystem_does_not_describe(): void
    {
        static::assertNull((new CSVSourceOpener(CSVFixtureContext::memory('id\n'), new CSVReadOptions()))->describe());
    }

    public function test_a_rebuilt_opener_keeps_the_dialects_it_learned(): void
    {
        $source = CSVFixtureContext::source('semicolon.csv');
        $dialects = new CSVDialects();
        $dialects->learn($source->uri(), new CSVDialect(',', '"', '\\'));
        $description = (new CSVSourceOpener(new NativeLocalFilesystem(), new CSVReadOptions(), $dialects))->describe();

        static::assertNotNull($description);

        $open = (new Rebuild())
            ->object(Description::fromArray($description->normalize()), CSVSourceOpener::class)
            ->open($source);

        try {
            static::assertSame(['id;name'], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_a_file_without_a_record_has_no_columns(): void
    {
        $open = (new CSVSourceOpener(
            new NativeLocalFilesystem(),
            new CSVReadOptions(),
        ))->open(CSVFixtureContext::source('empty.csv'));

        try {
            static::assertSame([], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_every_instance_opens_its_own_stream(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $source = CSVFixtureContext::source('header_only.csv');

        $first = (new CSVSourceOpener($filesystem, new CSVReadOptions()))->open($source);
        $second = (new CSVSourceOpener($filesystem, new CSVReadOptions()))->open($source);

        try {
            static::assertSame(['id', 'name'], $first->columns());
            static::assertSame(['id', 'name'], $second->columns());
        } finally {
            $first->close();
            $second->close();
        }
    }

    public function test_with_the_extension_the_rust_source_reads(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $open = (new CSVSourceOpener(CSVFixtureContext::memory("id,name\n1,a\n"), new CSVReadOptions()))->open(
            CSVFixtureContext::memorySource(),
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

        $open = (new CSVSourceOpener(CSVFixtureContext::memory("id,name\n1,a\n"), new CSVReadOptions()))->open(
            CSVFixtureContext::memorySource(),
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
        $open = (new CSVSourceOpener($filesystem, (new CSVReadOptions())->withCharactersReadInLine(4096)))->open(
            CSVFixtureContext::memorySource(),
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

    public function test_a_flow_php_of_another_abi_is_refused_before_the_file_is_opened(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        try {
            (new CSVSourceOpener(
                $filesystem,
                new CSVReadOptions(),
                extension: new FlowPhpExtension(true, FlowPhpExtension::ABI + 1, '0.46.0'),
            ))->open(CSVFixtureContext::source('header_only.csv'));
            static::fail('a skewed flow_php must be refused');
        } catch (ETLRuntimeException $e) {
            static::assertStringContainsString('does not match flow-php/etl', $e->getMessage());
        }

        static::assertSame(0, $filesystem->readFromCalls);
    }
}
