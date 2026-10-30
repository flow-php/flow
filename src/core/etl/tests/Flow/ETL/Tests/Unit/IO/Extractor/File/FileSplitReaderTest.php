<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplitReader;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\InMemoryFileBatches;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\FileReadMother;
use Flow\ETL\Values\Rows;

use function array_map;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class FileSplitReaderTest extends FlowTestCase
{
    public function test_a_read_goes_through_the_batches_and_closes_them_at_its_end(): void
    {
        $batches = new InMemoryFileBatches(['memory://a.csv' => [1, 2, 3]]);
        $reader = new FileSplitReader(
            FileReadMother::of(
                schema(int_schema('id')),
                batchSize: 2,
                filesystem: MemoryFiles::with(['memory://a.csv' => 'abc']),
                path: path('memory://a.csv'),
            ),
            $batches,
        );

        static::assertSame(
            [[['id' => 1], ['id' => 2]], [['id' => 3]]],
            array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array($reader->extract(flow_context()->backend(), new Criteria()), false),
            ),
        );
        static::assertSame(1, $batches->closes);
    }

    public function test_a_read_stopped_early_still_closes_the_batches(): void
    {
        $batches = new InMemoryFileBatches(['memory://a.csv' => [1, 2, 3]]);
        $read = (new FileSplitReader(
            FileReadMother::of(
                schema(int_schema('id')),
                batchSize: 2,
                filesystem: MemoryFiles::with(['memory://a.csv' => 'abc']),
                path: path('memory://a.csv'),
            ),
            $batches,
        ))->extract(flow_context()->backend(), new Criteria());

        $read->current();
        $read->send(Signal::STOP);

        static::assertFalse($read->valid());
        static::assertSame(1, $batches->closes);
    }

    public function test_batches_that_do_not_describe_leave_the_reader_undescribed(): void
    {
        static::assertNull(
            (new FileSplitReader(
                FileReadMother::of(
                    schema(int_schema('id')),
                    batchSize: 2,
                    filesystem: MemoryFiles::with(['memory://a.csv' => 'abc']),
                    path: path('memory://a.csv'),
                ),
                new InMemoryFileBatches([]),
            ))->describe(),
        );
    }
}
