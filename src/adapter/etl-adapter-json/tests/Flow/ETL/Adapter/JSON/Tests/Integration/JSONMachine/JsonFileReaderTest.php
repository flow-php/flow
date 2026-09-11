<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\JSONMachine;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonFormat;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Generator;
use JsonMachine\Exception\SyntaxErrorException;
use JsonMachine\Items;
use JsonMachine\JsonDecoder\ExtJsonDecoder;
use PHPUnit\Framework\Attributes\TestWith;

use function array_keys;
use function array_map;
use function array_slice;
use function count;
use function Flow\Types\DSL\type_array;
use function implode;
use function iterator_to_array;

final class JsonFileReaderTest extends FlowTestCase
{
    #[TestWith([JsonFormat::Document, 'five_rows.json'])]
    #[TestWith([JsonFormat::Lines, 'five_rows.jsonl'])]
    public function test_batches_are_sized_and_the_tail_is_shorter(JsonFormat $format, string $fixture): void
    {
        $reader = JsonFixtureContext::reader($format);

        $batches = iterator_to_array($reader->batches(JsonFixtureContext::source($fixture), 2), false);

        static::assertSame([2, 2, 1], array_map(count(...), $batches));
    }

    #[TestWith([JsonFormat::Document, 'five_rows.json'])]
    #[TestWith([JsonFormat::Lines, 'five_rows.jsonl'])]
    public function test_batches_closes_its_stream_when_abandoned(JsonFormat $format, string $fixture): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $reader = JsonFixtureContext::reader($format, $counting);

        $batches = $reader->batches(JsonFixtureContext::source($fixture), 2);
        static::assertCount(2, $batches->current());
        unset($batches);

        static::assertSame(1, $counting->readFromCalls);
        static::assertSame(1, $counting->closedStreams());
    }

    #[TestWith([JsonFormat::Document, 'empty_array.json'])]
    #[TestWith([JsonFormat::Document, 'array_of_empty_object.json'])]
    #[TestWith([JsonFormat::Document, 'empty.json'])]
    #[TestWith([JsonFormat::Lines, 'empty.jsonl'])]
    #[TestWith([JsonFormat::Lines, 'only_blank_lines.jsonl'])]
    public function test_batches_never_yields_an_empty_batch(JsonFormat $format, string $fixture): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $reader = JsonFixtureContext::reader($format, $counting);

        static::assertSame([], iterator_to_array($reader->batches(JsonFixtureContext::source($fixture), 2), false));
        static::assertSame(1, $counting->closedStreams());
    }

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
        ]), false), array_map(static fn(RawRowValues $values): array => $values->values, $rows));
    }

    #[TestWith([JsonFormat::Document, 'empty_record.json'])]
    #[TestWith([JsonFormat::Lines, 'empty_record.jsonl'])]
    public function test_an_empty_record_is_skipped(JsonFormat $format, string $fixture): void
    {
        $reader = JsonFixtureContext::reader($format);

        $rows = iterator_to_array($reader->sample(JsonFixtureContext::source($fixture)), false);

        static::assertSame(
            [['id' => 1], ['id' => 2]],
            array_map(static fn(RawRowValues $v): array => $v->values, $rows),
        );
    }

    public function test_a_zero_byte_document_yields_nothing(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $reader = JsonFixtureContext::reader(JsonFormat::Document, $counting);

        static::assertSame([], iterator_to_array($reader->sample(JsonFixtureContext::source('empty.json')), false));
        static::assertSame(1, $counting->closedStreams());
    }

    public function test_lines_without_a_pointer_make_the_line_the_row(): void
    {
        $filesystem = new NativeLocalFilesystem();
        $reader = JsonFixtureContext::reader(JsonFormat::Lines);

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
        $reader = JsonFixtureContext::reader(JsonFormat::Lines);

        $stream = $filesystem->readFrom(JsonFixtureContext::source($fixture)->path);
        $rows = iterator_to_array($reader->lineItems($stream), false);
        $stream->close();

        static::assertSame($expected, $rows);
    }

    public function test_a_bare_scalar_line_is_refused(): void
    {
        $filesystem = new NativeLocalFilesystem();
        $reader = JsonFixtureContext::reader(JsonFormat::Lines);
        $stream = $filesystem->readFrom(JsonFixtureContext::source('scalar_line.jsonl')->path);

        $this->expectException(SyntaxErrorException::class);
        $this->expectExceptionMessage("Unexpected symbol '5'");

        try {
            iterator_to_array($reader->lineItems($stream), false);
        } finally {
            $stream->close();
        }
    }

    #[TestWith([JsonFormat::Document, 'nested_timezones.json', '/timezones', 247, 'name', ['Aruba', 'Afghanistan']])]
    #[TestWith([JsonFormat::Lines, 'pointer_lines.jsonl', '/items', 3, 'a', [1, 2, 3]])]
    public function test_the_pointer_selects_the_rows(
        JsonFormat $format,
        string $fixture,
        string $pointer,
        int $expectedRows,
        string $expectedKey,
        array $expectedHead,
    ): void {
        $reader = JsonFixtureContext::reader($format, pointer: $pointer);

        $rows = iterator_to_array($reader->sample(JsonFixtureContext::source($fixture)), false);
        $values = array_map(static fn(RawRowValues $v): mixed => $v->values[$expectedKey], $rows);

        static::assertCount($expectedRows, $rows);
        static::assertSame($expectedHead, array_slice($values, 0, count($expectedHead)));
    }

    #[TestWith([JsonFormat::Document, 'nested_timezones.json', '/timezones', 247])]
    #[TestWith([JsonFormat::Lines, 'pointer_lines.jsonl', '/items', 3])]
    public function test_the_pointer_can_name_the_column(
        JsonFormat $format,
        string $fixture,
        string $pointer,
        int $expectedRows,
    ): void {
        $reader = JsonFixtureContext::reader($format, pointer: $pointer, pointerToEntryName: true);

        $rows = iterator_to_array($reader->sample(JsonFixtureContext::source($fixture)), false);

        static::assertCount($expectedRows, $rows);

        foreach ($rows as $row) {
            static::assertSame([$pointer], array_keys($row->values));
            static::assertIsArray($row->values[$pointer]);
        }
    }

    #[TestWith([JsonFormat::Document, 'five_rows.json'])]
    #[TestWith([JsonFormat::Lines, 'five_rows.jsonl'])]
    public function test_sample_closes_its_stream_when_abandoned(JsonFormat $format, string $fixture): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $reader = JsonFixtureContext::reader($format, $counting);

        $rows = $reader->sample(JsonFixtureContext::source($fixture));
        $first = $rows->current();
        unset($rows);

        static::assertNotNull($first);
        static::assertSame(['id' => 1], $first->values);

        static::assertSame(1, $counting->readFromCalls);
        static::assertSame(1, $counting->closedStreams());
    }

    #[TestWith([JsonFormat::Document, 'five_rows.json'])]
    #[TestWith([JsonFormat::Lines, 'five_rows.jsonl'])]
    public function test_sample_yields_every_row_of_a_source(JsonFormat $format, string $fixture): void
    {
        $reader = JsonFixtureContext::reader($format);

        $rows = iterator_to_array($reader->sample(JsonFixtureContext::source($fixture)), false);

        static::assertSame([1, 2, 3, 4, 5], array_map(static fn(RawRowValues $v): mixed => $v->values['id'], $rows));
    }

    #[TestWith([JsonFormat::Document, 'five_rows.json'])]
    #[TestWith([JsonFormat::Lines, 'five_rows.jsonl'])]
    public function test_samples_does_not_ration_the_row_budget(JsonFormat $format, string $fixture): void
    {
        $reader = JsonFixtureContext::reader($format, sources: JsonFixtureContext::sources($fixture));

        $samples = iterator_to_array($reader->samples(1), false);

        static::assertCount(1, $samples);
        static::assertCount(5, iterator_to_array($samples[0], false));
    }

    public function test_a_glob_reader_samples_every_listed_file_in_listing_order(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $reader = JsonFixtureContext::globReader('extra_key/*.json', JsonFormat::Document, $counting);

        $ids = [];

        foreach ($reader->samples(100) as $sample) {
            foreach ($sample as $values) {
                $ids[] = $values->values['id'];
            }
        }

        static::assertSame([1, 2, 3], $ids);
        static::assertSame(2, $counting->readFromCalls);
        static::assertSame(2, $counting->closedStreams());
    }

    public function test_samples_yields_one_unstarted_generator_per_source_in_listing_order(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $reader = JsonFixtureContext::reader(
            JsonFormat::Document,
            $counting,
            JsonFixtureContext::sources('five_rows.json', 'parity_people.json'),
        );

        $samples = iterator_to_array($reader->samples(10), false);
        $beforeAdvance = $counting->readFromCalls;

        $first = $samples[0]->current();
        $afterAdvance = $counting->readFromCalls;

        static::assertCount(2, $samples);
        static::assertContainsOnlyInstancesOf(Generator::class, $samples);
        static::assertSame(0, $beforeAdvance, 'samples() opens nothing until an inner generator is advanced');
        static::assertSame(1, $afterAdvance, 'advancing the first sample opens exactly the first source');
        static::assertNotNull($first);
        static::assertSame(['id' => 1], $first->values);
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
        $reader = JsonFixtureContext::reader(JsonFormat::Lines);

        $stream = $filesystem->readFrom(JsonFixtureContext::source($fixture)->path);
        $rows = iterator_to_array($reader->lineItems($stream), false);
        $stream->close();

        static::assertSame([['id' => 1], ['id' => 2]], $rows);
    }

    public function test_a_whitespace_only_line_is_skipped_under_a_pointer(): void
    {
        $reader = JsonFixtureContext::reader(JsonFormat::Lines, pointer: '/items');

        $rows = iterator_to_array($reader->sample(JsonFixtureContext::source('pointer_blank_line.jsonl')), false);

        static::assertSame([1, 2], array_map(static fn(RawRowValues $v): mixed => $v->values['a'], $rows));
    }

    public function test_a_nul_only_line_is_refused(): void
    {
        $filesystem = new NativeLocalFilesystem();
        $reader = JsonFixtureContext::reader(JsonFormat::Lines);
        $stream = $filesystem->readFrom(JsonFixtureContext::source('nul_line.jsonl')->path);

        $this->expectException(SyntaxErrorException::class);
        $this->expectExceptionMessage('Unexpected symbol');

        try {
            iterator_to_array($reader->lineItems($stream), false);
        } finally {
            $stream->close();
        }
    }
}
