<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\FloeMerger;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeWriter;
use Flow\Floe\Tests\Mother\RowsMother;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;
use function str_pad;

final class FloeStreamReaderTest extends FlowIntegrationTestCase
{
    public function test_merged_multi_schema_file_reads_seamlessly(): void
    {
        // schema evolution lives in FloeMerger; a merged multi-schema file must read seamlessly
        $path = $this->cacheDir->suffix('evolving.floe');
        $fileOne = $this->cacheDir->suffix('evolving-1.floe');
        $fileTwo = $this->cacheDir->suffix('evolving-2.floe');
        $fileThree = $this->cacheDir->suffix('evolving-3.floe');

        $rowsOne = rows(schema(int_schema('id')), row(['id' => 1]));
        $writer = new FloeWriter($this->fs(), $rowsOne->schema());
        $writer->create($fileOne);
        $writer->write($rowsOne);
        $writer->close();

        $rowsTwo = rows(
            schema(int_schema('id'), str_schema('email', nullable: true)),
            row(['id' => 2, 'email' => null]),
        );
        $writer = new FloeWriter($this->fs(), $rowsTwo->schema());
        $writer->create($fileTwo);
        $writer->write($rowsTwo);
        $writer->close();

        $rowsThree = rows(schema(int_schema('id'), str_schema('email')), row(['id' => 3, 'email' => 'third@flow.php']));
        $writer = new FloeWriter($this->fs(), $rowsThree->schema());
        $writer->create($fileThree);
        $writer->write($rowsThree);
        $writer->close();

        (new FloeMerger($this->fs()))->merge([$fileOne, $fileTwo, $fileThree], $path);

        $reader = (new FloeReader($this->fs()))->read($path);

        static::assertSame(3, $reader->totalRows());

        $read = [];

        foreach ($reader->rows() as $batch) {
            foreach ($batch->all() as $row) {
                static::assertSame(['id', 'email'], $row->names());
                $read[] = [$row->get('id'), $row->get('email')];
            }
        }

        static::assertSame([[1, null], [2, null], [3, 'third@flow.php']], $read);
    }

    public function test_file_larger_than_compaction_threshold_round_trips(): void
    {
        $path = $this->cacheDir->suffix('large.floe');

        $schema = rows(
            schema(int_schema('id'), str_schema('payload')),
            row(['id' => 0, 'payload' => str_pad('row_0', 300, 'x')]),
        )->schema();
        $writer = new FloeWriter($this->fs(), $schema);
        $writer->create($path);
        $written = 0;

        for ($i = 0; $i < 4000; $i++) {
            $writer->write(rows(
                schema(int_schema('id'), str_schema('payload')),
                row(['id' => $i, 'payload' => str_pad('row_' . $i, 300, 'x')]),
            ));
            $written++;
        }

        $writer->close();

        // The file must cross FrameReader's 1 MiB buffer-compaction threshold,
        // otherwise this test silently stops covering it.
        static::assertGreaterThan(1_048_576, $this->fs()->readFrom($path)->size());

        $read = 0;

        foreach ((new FloeReader($this->fs(), chunkSize: 4096))
            ->read($path)
            ->rows(500) as $batch) {
            foreach ($batch->all() as $row) {
                static::assertSame($read, $row->get('id'));
                $read++;
            }
        }

        static::assertSame($written, $read);
    }

    public function test_round_trip_of_all_entry_types_through_local_filesystem(): void
    {
        $rows = RowsMother::withAllEntryTypes();
        $path = $this->cacheDir->suffix('all-types.floe');

        $writer = new FloeWriter($this->fs(), $rows->schema());
        $writer->create($path);
        $writer->write($rows);
        $writer->close();

        $batches = iterator_to_array(
            (new FloeReader($this->fs()))
                ->read($path)
                ->rows(),
        );

        static::assertCount(1, $batches);
        static::assertEquals($rows->all(), $batches[0]->all());
    }

    public function test_round_trip_of_heterogeneous_rows_through_local_filesystem(): void
    {
        $rows = RowsMother::heterogeneous();
        $path = $this->cacheDir->suffix('heterogeneous.floe');

        $writer = new FloeWriter($this->fs(), $rows->schema());
        $writer->create($path);
        $writer->write($rows);
        $writer->close();

        $read = 0;

        foreach ((new FloeReader($this->fs()))
            ->read($path)
            ->rows() as $batch) {
            $read += $batch->count();
        }

        static::assertSame($rows->count(), $read);
    }
}
