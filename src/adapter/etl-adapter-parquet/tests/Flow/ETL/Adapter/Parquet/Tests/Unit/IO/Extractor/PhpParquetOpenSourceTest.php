<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\IO\Extractor;

use DateTimeImmutable;
use Flow\ETL\Adapter\Parquet\IO\Extractor\PhpParquetOpenSource;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Reader;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Parquet\DSL\parquet_column;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function iterator_to_array;
use function range;

final class PhpParquetOpenSourceTest extends FlowTestCase
{
    /**
     * @param list<int> $ids
     */
    #[TestWith([0, null, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])]
    #[TestWith([3, 5, [3, 4, 5, 6, 7]])]
    #[TestWith([6, null, [6, 7, 8, 9]])]
    #[TestWith([0, 1, [0]])]
    public function test_a_planned_window_crosses_row_groups(int $offset, ?int $limit, array $ids): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);

        $read = [];

        foreach (ParquetFilesContext::rows((new PhpParquetOpenSource((new Reader(
            engine: new PhpParquetEngine(),
        ))->readStream($filesystem->readFrom(path('memory://groups.parquet')))))->batches(
            schema(int_schema('id')),
            3,
            ParquetFilesContext::plan($filesystem, 'memory://groups.parquet', $offset, $limit),
            new PhpBackend(),
        )) as $rows) {
            static::assertLessThanOrEqual(3, $rows->count());
            $read = [...$read, ...$rows->column('id')->values()];
        }

        static::assertSame($ids, $read);
    }

    #[TestWith([0, null, 10])]
    #[TestWith([3, 5, 5])]
    public function test_an_empty_schema_yields_the_rows_of_the_window(int $offset, ?int $limit, int $count): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);

        $counted = 0;

        foreach (ParquetFilesContext::rows((new PhpParquetOpenSource((new Reader(
            engine: new PhpParquetEngine(),
        ))->readStream($filesystem->readFrom(path('memory://groups.parquet')))))->batches(
            schema(),
            3,
            ParquetFilesContext::plan($filesystem, 'memory://groups.parquet', $offset, $limit),
            new PhpBackend(),
        )) as $rows) {
            static::assertLessThanOrEqual(3, $rows->count());
            static::assertEquals(schema(), $rows->schema());
            $counted += $rows->count();
        }

        static::assertSame($count, $counted);
    }

    public function test_an_empty_schema_yields_the_rows_the_row_filter_keeps(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);

        $counted = 0;

        foreach (ParquetFilesContext::rows((new PhpParquetOpenSource((new Reader(
            engine: new PhpParquetEngine(),
        ))->readStream($filesystem->readFrom(path('memory://groups.parquet')))))->batches(
            schema(),
            100,
            ParquetFilesContext::plan(
                $filesystem,
                'memory://groups.parquet',
                filter: parquet_column('id')->greaterThanOrEqual(3),
            ),
            new PhpBackend(),
        )) as $rows) {
            $counted += $rows->count();
        }

        static::assertSame(7, $counted);
    }

    public function test_columns_declared_as_the_file_types_them_read_as_their_values(): void
    {
        $filesystem = memory_filesystem();
        $rows = array_to_rows(
            [[
                'd' => new DateTimeImmutable('2024-02-29 00:00:00 UTC'),
                'at' => new DateTimeImmutable('2009-02-13 23:31:30.123456 UTC'),
                'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
                'ats' => [new DateTimeImmutable('1969-12-31 23:59:59.999999 UTC')],
                's' => ['day' => new DateTimeImmutable('1969-12-31 00:00:00 UTC')],
            ]],
            schema(
                date_schema('d'),
                datetime_schema('at'),
                uuid_schema('id'),
                list_schema('ats', type_list(type_datetime())),
                structure_schema('s', type_structure(['day' => type_date()])),
            ),
        );
        df()
            ->read(from_rows($rows))
            ->write(to_parquet(path('memory://typed.parquet'), filesystem: $filesystem))
            ->run();

        static::assertEquals(
            $rows->toArray(),
            ParquetFilesContext::rows((new PhpParquetOpenSource((new Reader(
                engine: new PhpParquetEngine(),
            ))->readStream($filesystem->readFrom(path('memory://typed.parquet')))))->batches(
                $rows->schema(),
                100,
                ParquetFilesContext::plan($filesystem, 'memory://typed.parquet'),
                new PhpBackend(),
            ))[0]->toArray(),
        );
    }

    public function test_a_column_declared_as_another_type_is_cast_from_the_file_values(): void
    {
        $filesystem = memory_filesystem();
        $at = new DateTimeImmutable('2009-02-13 23:31:30.123456 UTC');
        df()
            ->read(from_rows(array_to_rows([['ats' => [$at]]], schema(list_schema('ats', type_list(type_datetime()))))))
            ->write(to_parquet(path('memory://typed.parquet'), filesystem: $filesystem))
            ->run();
        $declared = schema(list_schema('ats', type_list(type_string())));

        static::assertSame(
            array_to_rows([['ats' => [$at]]], $declared)->toArray(),
            ParquetFilesContext::rows((new PhpParquetOpenSource((new Reader(
                engine: new PhpParquetEngine(),
            ))->readStream($filesystem->readFrom(path('memory://typed.parquet')))))->batches(
                $declared,
                100,
                ParquetFilesContext::plan($filesystem, 'memory://typed.parquet'),
                new PhpBackend(),
            ))[0]->toArray(),
        );
    }

    public function test_batches_hold_the_schema_columns(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);
        $source = new PhpParquetOpenSource((new Reader(
            engine: new PhpParquetEngine(),
        ))->readStream($filesystem->readFrom(path('memory://groups.parquet'))));

        $names = [];

        foreach (ParquetFilesContext::rows($source->batches(
            schema(str_schema('name')),
            100,
            ParquetFilesContext::plan($filesystem, 'memory://groups.parquet'),
            new PhpBackend(),
        )) as $rows) {
            static::assertEquals(schema(str_schema('name')), $rows->schema());
            $names = [...$names, ...$rows->column('name')->values()];
        }

        $source->close();

        static::assertSame(array_map(static fn(int $id): string => 'n' . $id, range(0, 9)), $names);
    }

    public function test_a_column_the_file_lacks_is_refused(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Parquet file has no column "missing"');

        iterator_to_array((new PhpParquetOpenSource((new Reader(
            engine: new PhpParquetEngine(),
        ))->readStream($filesystem->readFrom(path('memory://groups.parquet')))))->batches(
            schema(int_schema('missing')),
            10,
            ParquetFilesContext::plan($filesystem, 'memory://groups.parquet'),
            new PhpBackend(),
        ));
    }

    public function test_a_batch_size_below_one_is_refused(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Parquet batch size must be greater than 0');

        iterator_to_array((new PhpParquetOpenSource((new Reader(
            engine: new PhpParquetEngine(),
        ))->readStream($filesystem->readFrom(path('memory://groups.parquet')))))->batches(
            schema(int_schema('id')),
            0,
            ParquetFilesContext::plan($filesystem, 'memory://groups.parquet'),
            new PhpBackend(),
        ));
    }
}
