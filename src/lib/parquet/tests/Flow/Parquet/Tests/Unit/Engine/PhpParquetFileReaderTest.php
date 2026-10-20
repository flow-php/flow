<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\PhpParquetFileReader;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\Reader\ReadPlan;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\RowSelector;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Double\InMemoryIndexes;
use Flow\Parquet\Tests\Double\ReadCountingSourceStream;
use Flow\Parquet\Tests\Mother\ReadPlanMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_column;
use function Flow\Parquet\DSL\parquet_column;
use function iterator_to_array;

final class PhpParquetFileReaderTest extends TestCase
{
    public function test_the_footer_is_read_once(): void
    {
        $stream = new ReadCountingSourceStream(new MemorySourceStream(MemoryParquetFile::threeRowGroups()));
        $reader = new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options());

        $footer = $reader->footer();
        $reads = $stream->reads;

        static::assertSame($footer, $reader->footer());
        static::assertSame(6, $footer->rowsNumber());
        static::assertSame($reads, $stream->reads);
    }

    /**
     * @param int<1, max> $batchSize
     * @param list<list<int>> $ids
     */
    #[TestWith([4, 0, null, [[1, 2, 3, 4], [5, 6]]])]
    #[TestWith([2, 2, 3, [[3, 4], [5]]])]
    #[TestWith([2, 6, null, []])]
    #[TestWith([2, 7, null, []])]
    public function test_read_columns_chunks_the_rows(int $batchSize, int $offset, ?int $limit, array $ids): void
    {
        $reader = new PhpParquetFileReader(
            new MemorySourceStream(MemoryParquetFile::threeRowGroups()),
            ByteOrder::LITTLE_ENDIAN,
            new Options(),
        );
        $chunks = iterator_to_array(
            $reader->readColumns(['id'], $batchSize, ReadPlanMother::window(6, $offset, $limit)),
            false,
        );

        static::assertSame($ids, array_column($chunks, 'id'));
    }

    public function test_a_row_group_the_plan_skips_is_never_read(): void
    {
        $full = new ReadCountingSourceStream(new MemorySourceStream(MemoryParquetFile::threeRowGroups()));
        $fullReader = new PhpParquetFileReader($full, ByteOrder::LITTLE_ENDIAN, new Options());
        $id = $fullReader->footer()->schema()->getFlat('id');
        iterator_to_array($fullReader->readColumns(['id'], 10, ReadPlanMother::window(6)), false);

        $pruned = new ReadCountingSourceStream(new MemorySourceStream(MemoryParquetFile::threeRowGroups()));
        $prunedReader = new PhpParquetFileReader($pruned, ByteOrder::LITTLE_ENDIAN, new Options());
        $selection = new RowSelection(new RowSelector(2, false), new RowSelector(2, true), new RowSelector(2, false));

        static::assertSame(
            [[1, 2, 5, 6]],
            array_column(
                iterator_to_array(
                    $prunedReader->readColumns(['id'], 10, new ReadPlan($selection, $selection, new InMemoryIndexes())),
                    false,
                ),
                'id',
            ),
        );
        static::assertSame(
            $full->bytes
            - $fullReader->footer()->metadata()->rowGroups()->all()[1]->getColumnChunk($id)->totalCompressedSize(),
            $pruned->bytes,
        );
    }

    /**
     * @param int<1, max> $batchSize
     * @param list<list<mixed>> $values
     */
    #[TestWith(['id', 10, [[2, 3, 6]]])]
    #[TestWith(['id', 2, [[2, 3], [6]]])]
    #[TestWith(['tags', 1, [[['t2']], [['t3']], [['t6']]]])]
    public function test_a_row_filter_returns_exactly_the_matching_rows_in_batches_across_row_groups(
        string $column,
        int $batchSize,
        array $values,
    ): void {
        $reader = new PhpParquetFileReader(
            new MemorySourceStream(MemoryParquetFile::threeRowGroups()),
            ByteOrder::LITTLE_ENDIAN,
            new Options(),
        );

        static::assertSame($values, array_column(
            iterator_to_array(
                $reader->readColumns(
                    [$column],
                    $batchSize,
                    ReadPlanMother::rowFiltered(6, parquet_column('id')->in([2, 3, 6]), $reader->footer()->schema()),
                ),
                false,
            ),
            $column,
        ));
    }

    public function test_a_limit_stops_the_read_inside_a_row_group_and_the_next_group_is_never_read(): void
    {
        $limited = new ReadCountingSourceStream(new MemorySourceStream(MemoryParquetFile::threeRowGroups()));
        $limitedReader = new PhpParquetFileReader($limited, ByteOrder::LITTLE_ENDIAN, new Options());
        $filter = parquet_column('id')->greaterThanOrEqual(2);

        static::assertSame(
            [[2, 3]],
            array_column(
                iterator_to_array(
                    $limitedReader->readColumns(
                        ['id'],
                        10,
                        ReadPlanMother::rowFiltered(6, $filter, $limitedReader->footer()->schema(), 2),
                    ),
                    false,
                ),
                'id',
            ),
        );

        $twoGroups = new ReadCountingSourceStream(new MemorySourceStream(MemoryParquetFile::threeRowGroups()));
        $twoGroupsReader = new PhpParquetFileReader($twoGroups, ByteOrder::LITTLE_ENDIAN, new Options());
        iterator_to_array(
            $twoGroupsReader->readColumns(
                ['id'],
                10,
                ReadPlanMother::rowFiltered(
                    6,
                    $filter,
                    $twoGroupsReader->footer()->schema(),
                    selection: RowSelection::window(6, 0, 4),
                ),
            ),
            false,
        );

        static::assertSame($twoGroups->bytes, $limited->bytes);
    }

    public function test_rows_proven_to_match_are_returned_without_evaluating_the_row_filter(): void
    {
        $reader = new PhpParquetFileReader(
            new MemorySourceStream(MemoryParquetFile::threeRowGroups()),
            ByteOrder::LITTLE_ENDIAN,
            new Options(),
        );

        static::assertSame(
            [[1, 2, 5, 6]],
            array_column(
                iterator_to_array(
                    $reader->readColumns(
                        ['id'],
                        10,
                        ReadPlanMother::rowFiltered(
                            6,
                            parquet_column('id')->greaterThan(4),
                            $reader->footer()->schema(),
                            matched: RowSelection::window(6, 0, 2),
                        ),
                    ),
                    false,
                ),
                'id',
            ),
        );
    }

    public function test_a_limit_of_zero_or_no_column_reads_nothing(): void
    {
        $reader = new PhpParquetFileReader(
            new MemorySourceStream(MemoryParquetFile::threeRowGroups()),
            ByteOrder::LITTLE_ENDIAN,
            new Options(),
        );

        static::assertSame(
            [],
            iterator_to_array(
                $reader->readColumns(
                    ['id'],
                    10,
                    ReadPlanMother::rowFiltered(6, parquet_column('id')->isNull(), $reader->footer()->schema(), 0),
                ),
                false,
            ),
        );
        static::assertSame([], iterator_to_array($reader->readColumns([], 10, ReadPlanMother::window(6)), false));
    }

    public function test_a_plan_over_another_row_count_is_refused(): void
    {
        $reader = new PhpParquetFileReader(
            new MemorySourceStream(MemoryParquetFile::threeRowGroups()),
            ByteOrder::LITTLE_ENDIAN,
            new Options(),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A read plan over 5 rows cannot read a file of 6 rows');

        iterator_to_array($reader->readColumns(['id'], 10, ReadPlanMother::window(5)));
    }

    public function test_close_closes_the_stream_and_every_later_call_throws_but_a_taken_footer_stays_readable(): void
    {
        $stream = new ReadCountingSourceStream(new MemorySourceStream(MemoryParquetFile::threeRowGroups()));
        $reader = new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options());
        $footer = $reader->footer();

        $reader->close();

        static::assertTrue($stream->closed);

        foreach ([
            static fn() => $reader->footer(),
            static fn() => iterator_to_array($reader->readColumns(['id'], 1, ReadPlanMother::window(6))),
            static fn() => $reader->close(),
        ] as $call) {
            try {
                $call();
                static::fail('A call on a closed reader must throw');
            } catch (RuntimeException $exception) {
                static::assertSame('Reader is not open', $exception->getMessage());
            }
        }
        static::assertSame(6, $footer->rowsNumber());
    }

    public function test_a_file_without_the_parquet_magic_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Given file is not valid Parquet file');

        (new PhpParquetFileReader(
            new MemorySourceStream('not a parquet file'),
            ByteOrder::LITTLE_ENDIAN,
            new Options(),
        ))->footer();
    }
}
