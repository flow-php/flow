<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Cardinality;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\Tests\Context\FloeFilesContext;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\to_timezone;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_string;
use function iterator_to_array;

final class FloeExtractorTest extends FlowIntegrationTestCase
{
    public function test_a_glob_declares_an_approximate_row_count(): void
    {
        FloeStreamReaderContext::write(
            $this->fs(),
            $this->cacheDir->suffix('glob/a.floe'),
            array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
        );
        FloeStreamReaderContext::write(
            $this->fs(),
            $this->cacheDir->suffix('glob/b.floe'),
            array_to_rows([['id' => 3]], schema(int_schema('id'))),
        );
        $firstBytes = FloeStreamReaderContext::dataBytes($this->fs(), $this->cacheDir->suffix('glob/a.floe'));

        $statistics = from_floe($this->cacheDir->suffix('glob/*.floe'), filesystem: $this->fs())->statistics();

        static::assertEquals(Cardinality::approximately(4, Cardinality::DEFAULT_RELATIVE_ERROR), $statistics->rows);
        static::assertEquals(
            Cardinality::approximately(2 * $firstBytes, Cardinality::DEFAULT_RELATIVE_ERROR),
            $statistics->size,
        );
    }

    public function test_a_single_file_declares_exact_rows_and_byte_size(): void
    {
        $path = $this->cacheDir->suffix('single.floe');
        FloeStreamReaderContext::write($this->fs(), $path, array_to_rows([
            ['id' => 1],
            ['id' => 2],
        ], schema(int_schema('id'))));

        $statistics = from_floe($path, filesystem: $this->fs())->statistics();

        static::assertEquals(Cardinality::exact(2), $statistics->rows);
        static::assertEquals(
            Cardinality::exact(FloeStreamReaderContext::dataBytes($this->fs(), $path)),
            $statistics->size,
        );
    }

    public function test_an_offset_is_subtracted_from_the_row_count(): void
    {
        $path = $this->cacheDir->suffix('offset.floe');
        FloeStreamReaderContext::write($this->fs(), $path, array_to_rows([
            ['id' => 1],
            ['id' => 2],
        ], schema(int_schema('id'))));

        $extractor = from_floe($path, filesystem: $this->fs());

        static::assertEquals(Cardinality::exact(2), $extractor->statistics()->rows);
        static::assertEquals(Cardinality::exact(1), $extractor->withOffset(1)->statistics()->rows);
        static::assertEquals(Cardinality::exact(0), $extractor->withOffset(10)->statistics()->rows);
    }

    public function test_statistics_are_computed_at_most_once(): void
    {
        $path = $this->cacheDir->suffix('memo.floe');
        FloeStreamReaderContext::write($this->fs(), $path, array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $filesystem = new CountingFilesystem($this->fs());
        $extractor = from_floe($path, filesystem: $filesystem);

        static::assertSame($extractor->statistics(), $extractor->statistics());
        static::assertSame(1, $filesystem->readFromCalls);
        static::assertSame(1, $filesystem->closedStreams());
    }

    public function test_schema_then_statistics_reads_the_footer_once(): void
    {
        $path = $this->cacheDir->suffix('schema-first.floe');
        FloeStreamReaderContext::write($this->fs(), $path, array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $filesystem = new CountingFilesystem($this->fs());
        $extractor = from_floe($path, filesystem: $filesystem);

        $extractor->schema();

        static::assertEquals(Cardinality::exact(1), $extractor->statistics()->rows);
        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_union_by_name_sums_every_footer_exactly(): void
    {
        FloeStreamReaderContext::write(
            $this->fs(),
            $this->cacheDir->suffix('union/a.floe'),
            array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
        );
        FloeStreamReaderContext::write(
            $this->fs(),
            $this->cacheDir->suffix('union/b.floe'),
            array_to_rows([['id' => 3]], schema(int_schema('id'))),
        );

        $statistics = from_floe($this->cacheDir->suffix('union/*.floe'), filesystem: $this->fs())
            ->unionByName()
            ->statistics();

        static::assertEquals(Cardinality::exact(3), $statistics->rows);
        static::assertEquals(
            Cardinality::exact(
                FloeStreamReaderContext::dataBytes($this->fs(), $this->cacheDir->suffix('union/a.floe'))
                + FloeStreamReaderContext::dataBytes($this->fs(), $this->cacheDir->suffix('union/b.floe')),
            ),
            $statistics->size,
        );
    }

    public function test_union_by_name_invalidates_the_memo(): void
    {
        $path = $this->cacheDir->suffix('union-memo.floe');
        FloeStreamReaderContext::write($this->fs(), $path, array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $filesystem = new CountingFilesystem($this->fs());
        $extractor = from_floe($path, filesystem: $filesystem);

        $extractor->statistics();
        $extractor->unionByName()->statistics();

        static::assertSame(2, $filesystem->readFromCalls);
    }

    public function test_a_utc_file_is_rezoned_after_reading(): void
    {
        $path = $this->cacheDir->suffix('utc.floe');
        FloeStreamReaderContext::write($this->fs(), $path, array_to_rows([['at' => new DateTimeImmutable(
            '2026-01-02 03:04:05',
            new DateTimeZone('UTC'),
        )]], schema(datetime_schema('at'))));

        static::assertSame(
            '2026-01-02 04:04:05 Europe/Warsaw',
            type_datetime()
                ->assert(
                    df()
                        ->read(from_floe($path, filesystem: $this->fs()))
                        ->withEntry('at', to_timezone(ref('at'), 'Europe/Warsaw'))
                        ->fetch()
                        ->column('at')
                        ->value(0),
                )
                ->format('Y-m-d H:i:s e'),
        );
    }

    public function test_two_files_read_in_the_first_footer_zone(): void
    {
        FloeStreamReaderContext::write(
            $this->fs(),
            $this->cacheDir->suffix('zones/a.floe'),
            array_to_rows([['at' => new DateTimeImmutable(
                '2026-01-02 03:04:05',
                new DateTimeZone('Europe/Warsaw'),
            )]], schema(datetime_schema('at', zone: 'Europe/Warsaw'))),
        );
        FloeStreamReaderContext::write(
            $this->fs(),
            $this->cacheDir->suffix('zones/b.floe'),
            array_to_rows([['at' => new DateTimeImmutable(
                '2026-01-02 03:04:05',
                new DateTimeZone('UTC'),
            )]], schema(datetime_schema('at'))),
        );

        $zones = [];

        foreach (df()
            ->read(from_floe($this->cacheDir->suffix('zones/*.floe'), filesystem: $this->fs()))
            ->fetch()
            ->toArray() as $row) {
            $zones[] = type_datetime()->assert($row['at'])->getTimezone()->getName();
        }

        static::assertSame(['Europe/Warsaw', 'Europe/Warsaw'], $zones);
    }

    public function test_xml_nodes_written_before_utf8_node_documents_read_back_and_load_as_their_text(): void
    {
        $loaded = $this->cacheDir->suffix('xml-node-utf8.floe');

        df()
            ->read(from_floe(__DIR__ . '/../Fixtures/xml-node-before-utf8.floe'))
            ->write(to_floe($loaded, filesystem: $this->fs()))
            ->run();

        static::assertSame(
            [
                ['node' => '<row><a>zażółć ☃ 😀</a></row>'],
                ['node' => '<row a="żółć"/>'],
                ['node' => '<row><b>&lt;p&gt; &amp; ©</b></row>'],
            ],
            df()
                ->read(from_floe($loaded, filesystem: $this->fs()))
                ->withEntry('node', ref('node')->cast(type_string()))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_an_offset_spanning_two_files_and_a_limit_inside_the_third_read_the_same_twice(): void
    {
        $filesystem = memory_filesystem();

        foreach (['a' => [1, 2, 3], 'b' => [4, 5, 6], 'c' => [7, 8, 9]] as $name => $ids) {
            df()
                ->read(from_array(array_map(static fn(int $id): array => ['id' => $id], $ids)))
                ->write(to_floe('memory://window/' . $name . '.floe', filesystem: $filesystem))
                ->run();
        }

        $extractor = from_floe('memory://window/*.floe', filesystem: $filesystem)->withOffset(4);
        $read = static fn(): array => array_merge(...array_map(
            static fn(Rows $rows): array => $rows->column('id')->values(),
            iterator_to_array($extractor->extract(flow_context(), limit: 4), false),
        ));

        static::assertSame([5, 6, 7, 8], $read());
        static::assertSame([5, 6, 7, 8], $read());
    }

    public function test_one_extractor_read_twice_interleaved_gives_each_read_every_row_and_closes_every_stream(): void
    {
        $filesystem = new CountingFilesystem(memory_filesystem());
        FloeFilesContext::writeFiles($filesystem, [
            'memory://in/a.floe' => array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
            'memory://in/b.floe' => array_to_rows([['id' => 3]], schema(int_schema('id'))),
        ]);
        [$first, $second] = ExtractedRows::interleaved(from_floe(
            path('memory://in/*.floe'),
            filesystem: $filesystem,
        )->withBatchSize(1));

        static::assertSame([['id' => 1], ['id' => 2], ['id' => 3]], $first->toArray());
        static::assertSame($first->toArray(), $second->toArray());
        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }
}
