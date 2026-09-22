<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\FloeWriter;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Floe\DSL\merge_floe;

final class FloeStatisticsTest extends FlowIntegrationTestCase
{
    public function test_a_compacted_merge_reports_the_same_rows_as_a_spliced_one(): void
    {
        $a = $this->cacheDir->suffix('compact-a.floe');
        $b = $this->cacheDir->suffix('compact-b.floe');
        $spliced = $this->cacheDir->suffix('compact-spliced.floe');
        $compacted = $this->cacheDir->suffix('compact-compacted.floe');
        FloeStreamReaderContext::write(
            $this->fs(),
            $a,
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
        );
        FloeStreamReaderContext::write($this->fs(), $b, rows(schema(int_schema('id')), row(['id' => 3])));

        merge_floe([$a, $b], $spliced);
        merge_floe([$a, $b], $compacted, compact: true);

        static::assertSame(3, FloeStreamReaderContext::footer($this->fs(), $spliced)->statistics->rows);
        static::assertSame(3, FloeStreamReaderContext::footer($this->fs(), $compacted)->statistics->rows);
        static::assertSame(
            FloeStreamReaderContext::dataBytes($this->fs(), $compacted),
            FloeStreamReaderContext::footer($this->fs(), $compacted)->statistics->byteSize,
        );
    }

    public function test_a_merged_file_reports_the_output_byte_size_not_the_inputs(): void
    {
        $a = $this->cacheDir->suffix('splice-a.floe');
        $b = $this->cacheDir->suffix('splice-b.floe');
        $out = $this->cacheDir->suffix('splice-out.floe');
        FloeStreamReaderContext::write($this->fs(), $a, rows(schema(str_schema('name')), row(['name' => 'first'])));
        FloeStreamReaderContext::write($this->fs(), $b, rows(schema(str_schema('name')), row(['name' => 'second'])));

        merge_floe([$a, $b], $out);

        $statistics = FloeStreamReaderContext::footer($this->fs(), $out)->statistics;

        static::assertSame(FloeStreamReaderContext::dataBytes($this->fs(), $out), $statistics->byteSize);
        static::assertSame(
            FloeStreamReaderContext::footer($this->fs(), $a)->statistics->byteSize
            + FloeStreamReaderContext::footer($this->fs(), $b)->statistics->byteSize,
            $statistics->byteSize,
        );
        static::assertLessThan(
            (int) $this->fs()->readFrom($a)->size() + (int) $this->fs()->readFrom($b)->size(),
            $statistics->byteSize,
        );
    }

    public function test_a_multi_section_file_sums_its_sections(): void
    {
        $path = $this->cacheDir->suffix('sections.floe');
        FloeStreamReaderContext::write($this->fs(), $path, rows(schema(int_schema('id')), row(['id' => 1])));

        foreach ([[2, 3], [4, 5, 6]] as $ids) {
            $writer = new FloeWriter($this->fs(), schema(int_schema('id')));
            $writer->append($path);

            foreach ($ids as $id) {
                $writer->write(rows(schema(int_schema('id')), row(['id' => $id])));
            }

            $writer->close();
        }

        $footer = FloeStreamReaderContext::footer($this->fs(), $path);
        $sectionRows = 0;

        foreach ($footer->sections as $section) {
            $sectionRows += $section->rowCount;
        }

        static::assertCount(3, $footer->sections);
        static::assertSame(6, $sectionRows);
        static::assertSame(6, $footer->statistics->rows);
        static::assertSame(FloeStreamReaderContext::dataBytes($this->fs(), $path), $footer->statistics->byteSize);
    }

    public function test_a_written_file_reports_its_rows_and_byte_size(): void
    {
        $path = $this->cacheDir->suffix('written.floe');
        FloeStreamReaderContext::write(
            $this->fs(),
            $path,
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
        );

        $statistics = FloeStreamReaderContext::footer($this->fs(), $path)->statistics;

        static::assertSame(2, $statistics->rows);
        static::assertSame(FloeStreamReaderContext::dataBytes($this->fs(), $path), $statistics->byteSize);
        static::assertGreaterThan(0, $statistics->byteSize);
    }

    public function test_an_appended_file_reports_the_combined_totals(): void
    {
        $path = $this->cacheDir->suffix('appended.floe');
        FloeStreamReaderContext::write($this->fs(), $path, rows(schema(int_schema('id')), row(['id' => 1])));
        $before = FloeStreamReaderContext::footer($this->fs(), $path)->statistics->byteSize;

        $writer = new FloeWriter($this->fs(), schema(int_schema('id')));
        $writer->append($path);
        $writer->write(rows(schema(int_schema('id')), row(['id' => 2]), row(['id' => 3])));
        $writer->close();

        $statistics = FloeStreamReaderContext::footer($this->fs(), $path)->statistics;

        static::assertSame(3, $statistics->rows);
        static::assertSame(FloeStreamReaderContext::dataBytes($this->fs(), $path), $statistics->byteSize);
        static::assertGreaterThan($before, $statistics->byteSize);
    }
}
