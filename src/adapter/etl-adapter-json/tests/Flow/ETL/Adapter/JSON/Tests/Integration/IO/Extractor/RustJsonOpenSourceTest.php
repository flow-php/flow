<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\IO\Extractor;

use Flow\ETL\Adapter\JSON\IO\Extractor\JsonFileBatches;
use Flow\ETL\Adapter\JSON\IO\Extractor\PhpJsonOpenSource;
use Flow\ETL\Adapter\JSON\IO\JsonFraming;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function in_array;
use function str_starts_with;

#[RequiresPhpExtension('flow_php')]
final class RustJsonOpenSourceTest extends FlowTestCase
{
    /**
     * Every fixture except the malformed ones RustJsonRefusalTest covers.
     *
     * @return Generator<string, array{string}>
     */
    public static function fixtures(): Generator
    {
        foreach (JsonFixtureContext::fixtures() as $label => [$fixture]) {
            if (!in_array($fixture, ['two_objects_on_a_line.jsonl', 'nul_line.jsonl', 'scalar_line.jsonl'], true)) {
                yield $label => [$fixture];
            }
        }
    }

    #[DataProvider('fixtures')]
    public function test_both_lanes_read_a_fixture_identically(string $fixture): void
    {
        // the orders fixtures (1 000 and 10 000 records) are sampled and read over their first 500 records only
        $orders = str_starts_with($fixture, 'orders_');
        $format = JsonFixtureContext::format($fixture);
        $schema = $orders
            ? JsonFixtureContext::infer($fixture, infer_schema()->sampleSize(500)->build())
            : JsonFixtureContext::infer($fixture);
        $source = JsonFixtureContext::source($fixture);
        $batchSize = $orders ? 100 : 2;
        $batches = $orders ? 5 : null;

        static::assertSame(
            JsonFixtureContext::outcome(
                new PhpJsonOpenSource(JsonFixtureContext::reader($format), $source),
                $schema,
                $batchSize,
                batches: $batches,
            ),
            JsonFixtureContext::outcome(
                JsonFixtureContext::open($format, $source),
                $schema,
                $batchSize,
                batches: $batches,
            ),
        );
    }

    /**
     * @return Generator<string, array{JsonFraming, string, Schema, ?int}>
     */
    public static function refusal_cases(): Generator
    {
        $id = schema(int_schema('id'));

        yield 'array element refused under a declared schema' => [
            JsonFraming::Array,
            '[{"id":1},{"id":2},{"id":3},{"id":"n/a"},{"id":5}]',
            $id,
            null,
        ];
        yield 'array element refused past the sample' => [JsonFraming::Array, '[{"id":1},{"id":"n/a"}]', $id, 1];
        yield 'lines refused and lacking a column under a declared schema' => [
            JsonFraming::Lines,
            "{\"id\":1}\r\n\n{\"id\":\"x\"}\n{\"other\":2}\n{\"id\":4}",
            $id,
            null,
        ];
        yield 'line refused past the sample' => [JsonFraming::Lines, "{\"id\":1}\n{\"id\":\"x\"}\n", $id, 1];
        yield 'line with a key past the sample' => [JsonFraming::Lines, "{\"id\":1}\n\n{\"id\":2,\"c\":3}\n", $id, 1];
    }

    #[DataProvider('refusal_cases')]
    public function test_both_lanes_refuse_a_record_identically(
        JsonFraming $format,
        string $content,
        Schema $schema,
        ?int $sampleSize,
    ): void {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://records.json'))->append($content)->close();
        $source = new SourceFile(path('memory://records.json'));

        $native = JsonFixtureContext::outcome(
            JsonFixtureContext::open($format, $source, $filesystem),
            $schema,
            2,
            sampleSize: $sampleSize,
        );

        static::assertStringNotContainsString(SchemaMismatchException::class, $native);
        static::assertSame(
            JsonFixtureContext::outcome(
                new PhpJsonOpenSource(JsonFixtureContext::reader($format, $filesystem), $source),
                $schema,
                2,
                sampleSize: $sampleSize,
            ),
            $native,
        );
    }

    public function test_a_window_past_the_first_byte_names_a_refused_line_by_offset_alone(): void
    {
        $filesystem = memory_filesystem();
        $filesystem
            ->writeTo(path('memory://records.jsonl'))
            ->append("{\"id\":1}\n{\"id\":2}\n{\"id\":\"x\"}\n")
            ->close();
        $source = new SourceFile(path('memory://records.jsonl'));
        $split = new FileSplit(path('memory://records.jsonl'), 9, null);
        $reader = JsonFixtureContext::reader(JsonFraming::Lines, $filesystem);

        $native = JsonFixtureContext::outcome(
            (new JsonFileBatches($filesystem, $reader))->open($source, $split),
            schema(int_schema('id')),
            2,
        );

        static::assertStringContainsString('i:1;i:18;i:2;N;', $native);
        static::assertSame(
            JsonFixtureContext::outcome(
                new PhpJsonOpenSource($reader, $source, $reader->window($split)),
                schema(int_schema('id')),
                2,
            ),
            $native,
        );
    }

    public function test_under_the_php_backend_no_native_column_survives(): void
    {
        $open = JsonFixtureContext::open(JsonFraming::Lines, JsonFixtureContext::source('parity_people.jsonl'));

        try {
            foreach ($open->batches(JsonFixtureContext::infer('parity_people.jsonl'), 2, new PhpBackend()) as $batch) {
                static::assertInstanceOf(Rows::class, $batch);

                foreach ($batch->columns() as $column) {
                    static::assertNotSame('Flow\ETL\Values\Column\RustColumn', $column::class);
                }
            }
        } finally {
            $open->close();
        }
    }

    public function test_close_after_a_partial_read_closes_the_stream(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $open = JsonFixtureContext::open(JsonFraming::Array, JsonFixtureContext::source('five_rows.json'), $counting);

        foreach ($open->batches(schema(int_schema('id')), 2, new PhpBackend()) as $rows) {
            static::assertInstanceOf(Rows::class, $rows);
            static::assertSame([['id' => 1], ['id' => 2]], $rows->toArray());

            break;
        }

        $open->close();

        static::assertSame(1, $counting->readFromCalls);
        static::assertSame(1, $counting->closedStreams());
    }
}
