<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Cardinality;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Floe\DSL\from_floe;

final class FloeExtractorTest extends FlowIntegrationTestCase
{
    public function test_a_glob_declares_an_approximate_row_count(): void
    {
        FloeStreamReaderContext::write(
            $this->fs(),
            $this->cacheDir->suffix('glob/a.floe'),
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
        );
        FloeStreamReaderContext::write(
            $this->fs(),
            $this->cacheDir->suffix('glob/b.floe'),
            rows(schema(int_schema('id')), row(['id' => 3])),
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
        FloeStreamReaderContext::write(
            $this->fs(),
            $path,
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
        );

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
        FloeStreamReaderContext::write(
            $this->fs(),
            $path,
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
        );

        $extractor = from_floe($path, filesystem: $this->fs());

        static::assertEquals(Cardinality::exact(2), $extractor->statistics()->rows);
        static::assertEquals(Cardinality::exact(1), $extractor->withOffset(1)->statistics()->rows);
        static::assertEquals(Cardinality::exact(0), $extractor->withOffset(10)->statistics()->rows);
    }

    public function test_statistics_are_computed_at_most_once(): void
    {
        $path = $this->cacheDir->suffix('memo.floe');
        FloeStreamReaderContext::write($this->fs(), $path, rows(schema(int_schema('id')), row(['id' => 1])));
        $filesystem = new CountingFilesystem($this->fs());
        $extractor = from_floe($path, filesystem: $filesystem);

        static::assertSame($extractor->statistics(), $extractor->statistics());
        static::assertSame(1, $filesystem->readFromCalls);
        static::assertSame(1, $filesystem->closedStreams());
    }

    public function test_schema_then_statistics_reads_the_footer_once(): void
    {
        $path = $this->cacheDir->suffix('schema-first.floe');
        FloeStreamReaderContext::write($this->fs(), $path, rows(schema(int_schema('id')), row(['id' => 1])));
        $filesystem = new CountingFilesystem($this->fs());
        $extractor = from_floe($path, filesystem: $filesystem);

        $extractor->schema();

        static::assertEquals(Cardinality::exact(1), $extractor->statistics()->rows);
        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_a_declared_schema_still_reads_the_footer_for_statistics(): void
    {
        $path = $this->cacheDir->suffix('declared.floe');
        FloeStreamReaderContext::write($this->fs(), $path, rows(schema(int_schema('id')), row(['id' => 1])));

        static::assertEquals(
            Cardinality::exact(1),
            from_floe($path, filesystem: $this->fs())->withSchema(schema(int_schema('id')))->statistics()->rows,
        );
    }

    public function test_union_by_name_sums_every_footer_exactly(): void
    {
        FloeStreamReaderContext::write(
            $this->fs(),
            $this->cacheDir->suffix('union/a.floe'),
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
        );
        FloeStreamReaderContext::write(
            $this->fs(),
            $this->cacheDir->suffix('union/b.floe'),
            rows(schema(int_schema('id')), row(['id' => 3])),
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
        FloeStreamReaderContext::write($this->fs(), $path, rows(schema(int_schema('id')), row(['id' => 1])));
        $filesystem = new CountingFilesystem($this->fs());
        $extractor = from_floe($path, filesystem: $filesystem);

        $extractor->statistics();
        $extractor->unionByName()->statistics();

        static::assertSame(2, $filesystem->readFromCalls);
    }
}
