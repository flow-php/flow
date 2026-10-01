<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonFormat;
use Flow\ETL\Adapter\JSON\NativeJsonOpenSource;
use Flow\ETL\Adapter\JSON\PhpJsonOpenSource;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\Attributes\TestWith;

use function extension_loaded;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;
use function serialize;
use function str_repeat;
use function strlen;

final class JsonSourceOpenerTest extends FlowTestCase
{
    #[TestWith([JsonFormat::Document, 'five_rows.json'])]
    #[TestWith([JsonFormat::Lines, 'five_rows.jsonl'])]
    public function test_without_the_extension_a_file_is_read_by_the_php_lane(JsonFormat $format, string $fixture): void
    {
        if (extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php reads this file natively');
        }

        static::assertInstanceOf(
            PhpJsonOpenSource::class,
            JsonFixtureContext::opener($format)->open(JsonFixtureContext::source($fixture)),
        );
    }

    #[TestWith([JsonFormat::Document, 'five_rows.json'])]
    #[TestWith([JsonFormat::Lines, 'five_rows.jsonl'])]
    public function test_batches_closes_the_source_when_the_consumer_stops_early(
        JsonFormat $format,
        string $fixture,
    ): void {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $opener = JsonFixtureContext::opener($format, $counting);

        $batches = $opener->batches(
            JsonFixtureContext::source($fixture),
            schema(int_schema('id')),
            2,
            new PhpBackend(),
        );
        static::assertCount(2, $batches->current());
        unset($batches);

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
        $open = JsonFixtureContext::opener($format, MemoryFiles::with([
            'memory://a.json' => $content,
        ]))->open(new SourceFile(path('memory://a.json')));
        $open->close();

        static::assertInstanceOf(NativeJsonOpenSource::class, $open);
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

        $open = JsonFixtureContext::opener(JsonFormat::Document, $counting)->open(new SourceFile(path(
            'memory://a.json',
        )));
        static::assertInstanceOf(NativeJsonOpenSource::class, $open);

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

        $open = JsonFixtureContext::opener(JsonFormat::Document, $counting)->open(new SourceFile(path(
            'memory://a.json',
        )));

        static::assertInstanceOf(PhpJsonOpenSource::class, $open);
        static::assertSame($counting->readFromCalls, $counting->closedStreams(), 'the peek closes its stream');
    }

    #[RequiresPhpExtension('flow_php')]
    #[TestWith([JsonFormat::Document, 'nested_timezones.json', '/timezones'])]
    #[TestWith([JsonFormat::Lines, 'pointer_lines.jsonl', '/items'])]
    public function test_a_pointer_is_read_by_the_php_lane(JsonFormat $format, string $fixture, string $pointer): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        $open = JsonFixtureContext::opener($format, $counting, $pointer)->open(JsonFixtureContext::source($fixture));

        static::assertInstanceOf(PhpJsonOpenSource::class, $open);
        static::assertSame(0, $counting->readFromCalls);
    }
}
