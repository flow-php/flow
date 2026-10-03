<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use Flow\ETL\Adapter\JSON\AdaptiveJsonOpenSource;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonFormat;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\FlowPhpExtension;
use Flow\ETL\RustIterator;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Generator;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\Attributes\TestWith;

use function extension_loaded;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;
use function serialize;
use function str_repeat;
use function strlen;

final class AdaptiveJsonOpenSourceTest extends FlowTestCase
{
    #[TestWith([JsonFormat::Document, 'five_rows.json'])]
    #[TestWith([JsonFormat::Lines, 'five_rows.jsonl'])]
    public function test_without_the_extension_a_file_is_read_by_the_php_lane(JsonFormat $format, string $fixture): void
    {
        if (extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php reads this file natively');
        }

        $open = JsonFixtureContext::open($format, JsonFixtureContext::source($fixture));

        static::assertInstanceOf(Generator::class, $open->batches(schema(int_schema('id')), 2, new PhpBackend()));
        $open->close();
    }

    #[TestWith([JsonFormat::Document, 'five_rows.json'])]
    #[TestWith([JsonFormat::Lines, 'five_rows.jsonl'])]
    public function test_close_closes_the_one_stream_the_source_opened(JsonFormat $format, string $fixture): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $open = JsonFixtureContext::open($format, JsonFixtureContext::source($fixture), $counting);

        foreach ($open->batches(schema(int_schema('id')), 2, new PhpBackend()) as $rows) {
            static::assertCount(2, $rows);

            break;
        }

        $open->close();

        static::assertSame(1, $counting->readFromCalls);
        static::assertSame(1, $counting->closedStreams());
    }

    #[RequiresPhpExtension('flow_php')]
    #[TestWith([JsonFormat::Lines, '{"id":1}'])]
    #[TestWith([JsonFormat::Lines, '{}'])]
    #[TestWith([JsonFormat::Document, "\u{FEFF} \n\t\r[{\"id\":1}]"])]
    #[TestWith([JsonFormat::Document, '[]'])]
    public function test_json_lines_and_a_top_level_array_are_read_natively(JsonFormat $format, string $content): void
    {
        $open = JsonFixtureContext::open($format, new SourceFile(path('memory://a.json')), MemoryFiles::with([
            'memory://a.json' => $content,
        ]));
        $open->close();

        static::assertInstanceOf(RustIterator::class, $open->batches(schema(int_schema('id')), 2, new PhpBackend()));
    }

    /**
     * @param non-negative-int $spaces
     */
    #[RequiresPhpExtension('flow_php')]
    #[TestWith(["\u{FEFF} [{\"id\":1},{\"id\":2}]"])]
    #[TestWith(['[{"id":1},{"id":2}]'])]
    #[TestWith([' [{"id":1},{"id":2}]', 40_000])]
    #[TestWith([' [{"id":1},{"id":2}]', 70_000])]
    public function test_a_native_document_is_read_once_from_the_peeked_chunk_on(string $content, int $spaces = 0): void
    {
        $content = str_repeat(' ', $spaces) . $content;
        $counting = new CountingFilesystem(MemoryFiles::with(['memory://a.json' => $content]));

        $open = JsonFixtureContext::open(JsonFormat::Document, new SourceFile(path('memory://a.json')), $counting);
        static::assertInstanceOf(RustIterator::class, $open->batches(schema(int_schema('id')), 2, new PhpBackend()));

        static::assertSame(
            serialize([[['id' => 1], ['id' => 2]]]),
            JsonFixtureContext::outcome($open, schema(int_schema('id')), 2),
        );
        static::assertSame(strlen($content), $counting->openedStreams[0]->readBytes);
    }

    #[RequiresPhpExtension('flow_php')]
    #[TestWith(['{"items":[{"id":1}]}'])]
    #[TestWith([" \n\t"])]
    #[TestWith([''])]
    #[TestWith(['5'])]
    public function test_a_document_not_opening_an_array_is_read_by_the_php_lane(string $content): void
    {
        $counting = new CountingFilesystem(MemoryFiles::with(['memory://a.json' => $content]));

        $open = JsonFixtureContext::open(JsonFormat::Document, new SourceFile(path('memory://a.json')), $counting);

        static::assertInstanceOf(Generator::class, $open->batches(schema(int_schema('id')), 2, new PhpBackend()));
        static::assertSame($counting->readFromCalls, $counting->closedStreams(), 'the peek closes its stream');
    }

    #[RequiresPhpExtension('flow_php')]
    #[TestWith([JsonFormat::Document, 'nested_timezones.json', '/timezones'])]
    #[TestWith([JsonFormat::Lines, 'pointer_lines.jsonl', '/items'])]
    public function test_a_pointer_is_read_by_the_php_lane(JsonFormat $format, string $fixture, string $pointer): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        $open = JsonFixtureContext::open($format, JsonFixtureContext::source($fixture), $counting, $pointer);

        static::assertInstanceOf(Generator::class, $open->batches(schema(int_schema('id')), 2, new PhpBackend()));
        static::assertSame(0, $counting->readFromCalls);
    }

    public function test_a_flow_php_of_another_abi_is_refused_before_the_file_is_opened(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        try {
            new AdaptiveJsonOpenSource(
                $filesystem,
                JsonFixtureContext::reader(JsonFormat::Lines, $filesystem),
                JsonFormat::Lines,
                null,
                JsonFixtureContext::source('five_rows.jsonl'),
                new FlowPhpExtension(true, FlowPhpExtension::ABI + 1, '0.46.0'),
            );
            static::fail('a skewed flow_php must be refused');
        } catch (RuntimeException $e) {
            static::assertStringContainsString('does not match flow-php/etl', $e->getMessage());
        }

        static::assertSame(0, $filesystem->readFromCalls);
    }

    public function test_without_flow_php_a_file_is_read_by_the_php_lane_whatever_is_loaded(): void
    {
        $open = new AdaptiveJsonOpenSource(
            new NativeLocalFilesystem(),
            JsonFixtureContext::reader(JsonFormat::Lines, new NativeLocalFilesystem()),
            JsonFormat::Lines,
            null,
            JsonFixtureContext::source('five_rows.jsonl'),
            new FlowPhpExtension(false, null),
        );

        static::assertInstanceOf(Generator::class, $open->batches(schema(int_schema('id')), 2, new PhpBackend()));
        $open->close();
    }
}
