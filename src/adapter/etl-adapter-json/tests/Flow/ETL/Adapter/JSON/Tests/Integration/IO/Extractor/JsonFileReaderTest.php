<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\IO\Extractor;

use Flow\ETL\Adapter\JSON\IO\Extractor\JsonFileReader;
use Flow\ETL\Adapter\JSON\IO\JsonFraming;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use JsonMachine\Exception\SyntaxErrorException;
use JsonMachine\Items;
use JsonMachine\JsonDecoder\ExtJsonDecoder;
use PHPUnit\Framework\Attributes\TestWith;

use function array_keys;
use function array_map;
use function array_slice;
use function count;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_array;
use function implode;
use function iterator_to_array;

final class JsonFileReaderTest extends FlowTestCase
{
    public function test_chunks_starts_with_the_first_chunk_then_the_rest(): void
    {
        $filesystem = new NativeLocalFilesystem();
        $reader = JsonFixtureContext::reader();
        $source = JsonFixtureContext::source('timezones.json');

        $stream = $filesystem->readFrom($source->path);
        $chunks = iterator_to_array($reader->chunks($stream, $stream->read(8192, 0)), false);
        $stream->close();

        $whole = $filesystem->readFrom($source->path);
        $content = $whole->content();
        $whole->close();

        static::assertCount(7, $chunks);
        static::assertSame($content, implode('', $chunks));
    }

    public function test_a_document_is_read_across_chunk_boundaries(): void
    {
        $reader = JsonFixtureContext::reader();
        $source = JsonFixtureContext::source('timezones.json');

        $rows = iterator_to_array($reader->sample($source), false);

        static::assertCount(247, $rows);
        static::assertSame(iterator_to_array(Items::fromFile($source->path->path(), [
            'decoder' => new ExtJsonDecoder(true),
        ]), false), array_map(static fn(array $values): array => $values, $rows));
    }

    #[TestWith([JsonFraming::Array, 'empty_record.json'])]
    #[TestWith([JsonFraming::Lines, 'empty_record.jsonl'])]
    public function test_an_empty_record_is_skipped(JsonFraming $format, string $fixture): void
    {
        $reader = JsonFixtureContext::reader($format);

        $rows = iterator_to_array($reader->sample(JsonFixtureContext::source($fixture)), false);

        static::assertSame([['id' => 1], ['id' => 2]], array_map(static fn(array $v): array => $v, $rows));
    }

    public function test_a_zero_byte_document_yields_nothing(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $reader = JsonFixtureContext::reader(JsonFraming::Array, $counting);

        static::assertSame([], iterator_to_array($reader->sample(JsonFixtureContext::source('empty.json')), false));
        static::assertSame(1, $counting->closedStreams());
    }

    public function test_lines_without_a_pointer_make_the_line_the_row(): void
    {
        $filesystem = new NativeLocalFilesystem();
        $reader = JsonFixtureContext::reader(JsonFraming::Lines);

        $stream = $filesystem->readFrom(JsonFixtureContext::source('five_rows.jsonl')->path);
        $rows = iterator_to_array($reader->lineItems($stream), false);
        $stream->close();

        static::assertSame(
            [['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]],
            array_map(static fn(mixed $row): array => type_array()->assert($row), $rows),
        );
    }

    #[TestWith(['bom_first_line.jsonl', [['id' => 1], ['id' => 2]]])]
    #[TestWith(['two_objects_on_a_line.jsonl', [['id' => 1], ['id' => 2]]])]
    /**
     * @param list<array<string, int>> $expected
     */
    public function test_a_line_json_decode_rejects_keeps_the_json_machine_reading(
        string $fixture,
        array $expected,
    ): void {
        $filesystem = new NativeLocalFilesystem();
        $reader = JsonFixtureContext::reader(JsonFraming::Lines);

        $stream = $filesystem->readFrom(JsonFixtureContext::source($fixture)->path);
        $rows = iterator_to_array($reader->lineItems($stream), false);
        $stream->close();

        static::assertSame($expected, $rows);
    }

    public function test_a_bare_scalar_line_is_refused(): void
    {
        $filesystem = new NativeLocalFilesystem();
        $reader = JsonFixtureContext::reader(JsonFraming::Lines);
        $stream = $filesystem->readFrom(JsonFixtureContext::source('scalar_line.jsonl')->path);

        $this->expectException(SyntaxErrorException::class);
        $this->expectExceptionMessage("Unexpected symbol '5'");

        try {
            iterator_to_array($reader->lineItems($stream), false);
        } finally {
            $stream->close();
        }
    }

    #[TestWith([JsonFraming::Array, 'nested_timezones.json', '/timezones', 247, 'name', ['Aruba', 'Afghanistan']])]
    #[TestWith([JsonFraming::Lines, 'pointer_lines.jsonl', '/items', 3, 'a', [1, 2, 3]])]
    public function test_the_pointer_selects_the_rows(
        JsonFraming $format,
        string $fixture,
        string $pointer,
        int $expectedRows,
        string $expectedKey,
        array $expectedHead,
    ): void {
        $reader = JsonFixtureContext::reader($format, pointer: $pointer);

        $rows = iterator_to_array($reader->sample(JsonFixtureContext::source($fixture)), false);
        $values = array_map(static fn(array $v): mixed => $v[$expectedKey], $rows);

        static::assertCount($expectedRows, $rows);
        static::assertSame($expectedHead, array_slice($values, 0, count($expectedHead)));
    }

    #[TestWith([JsonFraming::Array, 'nested_timezones.json', '/timezones', 247])]
    #[TestWith([JsonFraming::Lines, 'pointer_lines.jsonl', '/items', 3])]
    public function test_the_pointer_can_name_the_column(
        JsonFraming $format,
        string $fixture,
        string $pointer,
        int $expectedRows,
    ): void {
        $reader = JsonFixtureContext::reader($format, pointer: $pointer, pointerToColumnName: true);

        $rows = iterator_to_array($reader->sample(JsonFixtureContext::source($fixture)), false);

        static::assertCount($expectedRows, $rows);

        foreach ($rows as $row) {
            static::assertSame([$pointer], array_keys($row));
            static::assertIsArray($row[$pointer]);
        }
    }

    #[TestWith([JsonFraming::Array, 'five_rows.json'])]
    #[TestWith([JsonFraming::Lines, 'five_rows.jsonl'])]
    public function test_sample_closes_its_stream_when_abandoned(JsonFraming $format, string $fixture): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $reader = JsonFixtureContext::reader($format, $counting);

        $rows = $reader->sample(JsonFixtureContext::source($fixture));
        $first = $rows->current();
        unset($rows);

        static::assertNotNull($first);
        static::assertSame(['id' => 1], $first);

        static::assertSame(1, $counting->readFromCalls);
        static::assertSame(1, $counting->closedStreams());
    }

    #[TestWith([JsonFraming::Array, 'five_rows.json'])]
    #[TestWith([JsonFraming::Lines, 'five_rows.jsonl'])]
    public function test_sample_yields_every_row_of_a_source(JsonFraming $format, string $fixture): void
    {
        $reader = JsonFixtureContext::reader($format);

        $rows = iterator_to_array($reader->sample(JsonFixtureContext::source($fixture)), false);

        static::assertSame([1, 2, 3, 4, 5], array_map(static fn(array $v): mixed => $v['id'], $rows));
    }

    #[TestWith(['blank_line.jsonl'])]
    #[TestWith(['whitespace_line.jsonl'])]
    #[TestWith(['crlf_blank_line.jsonl'])]
    #[TestWith(['trailing_blank_lines.jsonl'])]
    #[TestWith(['vertical_tab_line.jsonl'])]
    #[TestWith(['form_feed_line.jsonl'])]
    public function test_a_whitespace_only_line_is_skipped(string $fixture): void
    {
        $filesystem = new NativeLocalFilesystem();
        $reader = JsonFixtureContext::reader(JsonFraming::Lines);

        $stream = $filesystem->readFrom(JsonFixtureContext::source($fixture)->path);
        $rows = iterator_to_array($reader->lineItems($stream), false);
        $stream->close();

        static::assertSame([['id' => 1], ['id' => 2]], $rows);
    }

    public function test_a_whitespace_only_line_is_skipped_under_a_pointer(): void
    {
        $reader = JsonFixtureContext::reader(JsonFraming::Lines, pointer: '/items');

        $rows = iterator_to_array($reader->sample(JsonFixtureContext::source('pointer_blank_line.jsonl')), false);

        static::assertSame([1, 2], array_map(static fn(array $v): mixed => $v['a'], $rows));
    }

    #[TestWith(['[1, {"id":1}]', null])]
    #[TestWith(['{"id":1}', null])]
    #[TestWith(['{"items":[1]}', '/items'])]
    public function test_a_scalar_record_is_refused(string $content, ?string $pointer): void
    {
        $filesystem = MemoryFiles::with(['memory://scalar.json' => $content]);
        $reader = JsonFixtureContext::reader(JsonFraming::Array, $filesystem, pointer: $pointer);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'A JSON record must be an object or an array, int given in "memory://scalar.json".',
        );

        iterator_to_array($reader->sample(new SourceFile(path('memory://scalar.json'))), false);
    }

    public function test_a_nul_only_line_is_refused(): void
    {
        $filesystem = new NativeLocalFilesystem();
        $reader = JsonFixtureContext::reader(JsonFraming::Lines);
        $stream = $filesystem->readFrom(JsonFixtureContext::source('nul_line.jsonl')->path);

        $this->expectException(SyntaxErrorException::class);
        $this->expectExceptionMessage('Unexpected symbol');

        try {
            iterator_to_array($reader->lineItems($stream), false);
        } finally {
            $stream->close();
        }
    }

    public function test_a_reader_over_the_local_filesystem_describes_and_rebuilds_equal(): void
    {
        $reader = new JsonFileReader(new NativeLocalFilesystem(), JsonFraming::Lines, '/items', true);
        $description = $reader->describe();

        static::assertNotNull($description);
        static::assertSame(
            ['format' => 'Lines', 'pointer' => '/items', 'pointer_to_column_name' => true],
            array_slice($description->data, 1),
        );
        static::assertEquals($reader, (new Rebuild())->object(
            Description::fromArray($description->normalize()),
            JsonFileReader::class,
        ));
    }

    public function test_a_reader_over_a_memory_filesystem_does_not_describe(): void
    {
        static::assertNull((new JsonFileReader(memory_filesystem(), JsonFraming::Array, null, false))->describe());
    }

    public function test_the_reader_declares_json(): void
    {
        static::assertSame(
            ['json'],
            (new JsonFileReader(new NativeLocalFilesystem(), JsonFraming::Array, null, false))
                ->describe()
                ?->extensions(),
        );
    }

    public function test_points_into_when_a_pointer_is_set(): void
    {
        static::assertTrue(
            (new JsonFileReader(new NativeLocalFilesystem(), JsonFraming::Array, '/items', false))->pointsInto(),
        );
        static::assertFalse(
            (new JsonFileReader(new NativeLocalFilesystem(), JsonFraming::Array, null, false))->pointsInto(),
        );
    }

    public function test_a_window_narrows_only_lines_and_never_the_split_of_a_whole_file(): void
    {
        $lines = new JsonFileReader(new NativeLocalFilesystem(), JsonFraming::Lines, null, false);
        $split = new FileSplit(path('file:///a.jsonl'), 10, 5);

        static::assertSame($split, $lines->window($split));
        static::assertNull($lines->window(null));
        static::assertNull($lines->window(new FileSplit(path('file:///a.jsonl'), 0, null)));
        static::assertNull((new JsonFileReader(new NativeLocalFilesystem(), JsonFraming::Array, null, false))->window(
            $split,
        ));
    }
}
