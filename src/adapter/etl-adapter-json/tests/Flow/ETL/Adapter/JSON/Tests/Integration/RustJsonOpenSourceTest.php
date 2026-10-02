<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonFormat;
use Flow\ETL\Adapter\JSON\PhpJsonOpenSource;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
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

    public function test_a_value_past_the_sample_is_refused_as_the_php_lane_refuses_it(): void
    {
        $schema = JsonFixtureContext::infer('misfit.json', infer_schema()->sampleSize(3)->build());
        $source = JsonFixtureContext::source('misfit.json');

        $native = JsonFixtureContext::outcome(JsonFixtureContext::open(JsonFormat::Document, $source), $schema, 2);

        static::assertStringStartsWith(SchemaMismatchException::class . ':', $native);
        static::assertSame(
            JsonFixtureContext::outcome(new PhpJsonOpenSource(JsonFixtureContext::reader(), $source), $schema, 2),
            $native,
        );
    }

    public function test_under_the_php_backend_no_native_column_survives(): void
    {
        $open = JsonFixtureContext::open(JsonFormat::Lines, JsonFixtureContext::source('parity_people.jsonl'));

        try {
            foreach ($open->batches(JsonFixtureContext::infer('parity_people.jsonl'), 2, new PhpBackend()) as $batch) {
                foreach ($batch->columns() as $column) {
                    static::assertNotSame('Flow\ETL\Column\RustColumn', $column::class);
                }
            }
        } finally {
            $open->close();
        }
    }

    public function test_close_after_a_partial_read_closes_the_stream(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $open = JsonFixtureContext::open(JsonFormat::Document, JsonFixtureContext::source('five_rows.json'), $counting);

        foreach ($open->batches(schema(int_schema('id')), 2, new PhpBackend()) as $rows) {
            static::assertSame([['id' => 1], ['id' => 2]], $rows->toArray());

            break;
        }

        $open->close();

        static::assertSame(1, $counting->readFromCalls);
        static::assertSame(1, $counting->closedStreams());
    }
}
