<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\FloeMerger;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeWriter;
use Flow\Floe\Tests\Mother\RowsMother;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
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

        $rowsOne = array_to_rows([['id' => 1]], schema(int_schema('id')));
        $writer = new FloeWriter($this->fs(), $rowsOne->schema(), new AdaptiveBackend());
        $writer->create($fileOne);
        $writer->write($rowsOne);
        $writer->close();

        $rowsTwo = array_to_rows(
            [['id' => 2, 'email' => null]],
            schema(int_schema('id'), str_schema('email', nullable: true)),
        );
        $writer = new FloeWriter($this->fs(), $rowsTwo->schema(), new AdaptiveBackend());
        $writer->create($fileTwo);
        $writer->write($rowsTwo);
        $writer->close();

        $rowsThree = array_to_rows(
            [['id' => 3, 'email' => 'third@flow.php']],
            schema(int_schema('id'), str_schema('email')),
        );
        $writer = new FloeWriter($this->fs(), $rowsThree->schema(), new AdaptiveBackend());
        $writer->create($fileThree);
        $writer->write($rowsThree);
        $writer->close();

        (new FloeMerger($this->fs(), new AdaptiveBackend()))->merge([$fileOne, $fileTwo, $fileThree], $path);

        $reader = (new FloeReader($this->fs(), new AdaptiveBackend()))->read($path);

        static::assertSame(3, $reader->totalRows());

        $read = [];

        foreach ($reader->rows() as $batch) {
            foreach ($batch->toArray() as $row) {
                static::assertSame(['id', 'email'], array_keys($row));
                $read[] = [$row['id'], $row['email']];
            }
        }

        static::assertSame([[1, null], [2, null], [3, 'third@flow.php']], $read);
    }

    public function test_file_larger_than_compaction_threshold_round_trips(): void
    {
        $path = $this->cacheDir->suffix('large.floe');

        $schema = array_to_rows(
            [['id' => 0, 'payload' => str_pad('row_0', 300, 'x')]],
            schema(int_schema('id'), str_schema('payload')),
        )->schema();
        $writer = new FloeWriter($this->fs(), $schema, new AdaptiveBackend());
        $writer->create($path);
        $written = 0;

        for ($i = 0; $i < 4000; $i++) {
            $writer->write(array_to_rows(
                [['id' => $i, 'payload' => str_pad('row_' . $i, 300, 'x')]],
                schema(int_schema('id'), str_schema('payload')),
            ));
            $written++;
        }

        $writer->close();

        // The file must cross FrameReader's 1 MiB buffer-compaction threshold,
        // otherwise this test silently stops covering it.
        static::assertGreaterThan(1_048_576, $this->fs()->readFrom($path)->size());

        $read = 0;

        foreach ((new FloeReader($this->fs(), new AdaptiveBackend(), chunkSize: 4096))
            ->read($path)
            ->rows(500) as $batch) {
            foreach ($batch->toArray() as $row) {
                static::assertSame($read, $row['id']);
                $read++;
            }
        }

        static::assertSame($written, $read);
    }

    public function test_round_trip_of_all_entry_types_through_local_filesystem(): void
    {
        $rows = RowsMother::withAllEntryTypes();
        $path = $this->cacheDir->suffix('all-types.floe');

        $writer = new FloeWriter($this->fs(), $rows->schema(), new AdaptiveBackend());
        $writer->create($path);
        $writer->write($rows);
        $writer->close();

        $batches = iterator_to_array(
            (new FloeReader($this->fs(), new AdaptiveBackend()))
                ->read($path)
                ->rows(),
        );

        static::assertCount(1, $batches);
        static::assertEquals($rows->toArray(), $batches[0]->toArray());
    }

    public function test_round_trip_of_heterogeneous_rows_through_local_filesystem(): void
    {
        $rows = RowsMother::heterogeneous();
        $path = $this->cacheDir->suffix('heterogeneous.floe');

        $writer = new FloeWriter($this->fs(), $rows->schema(), new AdaptiveBackend());
        $writer->create($path);
        $writer->write($rows);
        $writer->close();

        $read = 0;

        foreach ((new FloeReader($this->fs(), new AdaptiveBackend()))
            ->read($path)
            ->rows() as $batch) {
            $read += $batch->count();
        }

        static::assertSame($rows->count(), $read);
    }
}
