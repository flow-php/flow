<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetEngine;
use Flow\Parquet\Reader;
use PHPUnit\Framework\Attributes\DataProvider;

class ListsReadingTest extends ParquetIntegrationTestCase
{
    #[DataProvider('engine_provider')]
    public function test_reading_list_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/lists.parquet');

        static::assertNull($file->metadata()->schema()->get('list')->type());
        static::assertEquals('LIST', $file->metadata()->schema()->get('list')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['list']) as $row) {
            /** @var array<array-key, mixed> $list */
            $list = $row['list'];
            static::assertIsArray($list);
            static::assertContainsOnlyInt($list);
            static::assertCount(3, $list);
            $count++;
        }

        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_list_column_with_limit(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/lists.parquet');

        static::assertNull($file->metadata()->schema()->get('list')->type());
        static::assertEquals('LIST', $file->metadata()->schema()->get('list')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['list'], $limit = 50) as $row) {
            /** @var array<array-key, mixed> $list */
            $list = $row['list'];
            static::assertIsArray($list);
            static::assertContainsOnlyInt($list);
            static::assertCount(3, $list);
            $count++;
        }

        static::assertSame($limit, $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_list_nested_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/lists.parquet');

        static::assertNull($file->metadata()->schema()->get('list_nested')->type());
        static::assertEquals('LIST', $file->metadata()->schema()->get('list_nested')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['list_nested']) as $row) {
            /** @var array<array-key, mixed> $outer */
            $outer = $row['list_nested'];
            static::assertIsArray($outer);
            static::assertIsList($outer);
            /** @var array<array-key, mixed> $inner */
            $inner = $outer[0];
            static::assertIsArray($inner);
            static::assertIsList($inner);
            /** @var array<array-key, mixed> $innermost */
            $innermost = $inner[0];
            static::assertIsArray($innermost);
            static::assertIsArray($innermost);

            $count++;
        }
        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_list_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/lists.parquet');

        static::assertNull($file->metadata()->schema()->get('list_nullable')->type());
        static::assertEquals('LIST', $file->metadata()->schema()->get('list_nullable')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['list_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                /** @var array<array-key, mixed> $list */
                $list = $row['list_nullable'];
                static::assertIsArray($list);
                static::assertContainsOnlyInt($list);
                static::assertCount(3, $list);
            } else {
                static::assertNull($row['list_nullable']);
            }
            $count++;
        }

        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_list_of_structures_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/lists.parquet');

        static::assertNull($file->metadata()->schema()->get('list_mixed_types')->type());
        static::assertEquals('LIST', $file->metadata()->schema()->get('list_mixed_types')->logicalType()?->name());

        $count = 0;

        foreach ($file->values(['list_mixed_types']) as $row) {
            /** @var array<array-key, mixed> $list */
            $list = $row['list_mixed_types'];
            static::assertIsArray($list);
            static::assertCount(4, $list);

            for ($i = 0; $i < 4; $i++) {
                /** @var array<array-key, mixed> $entry */
                $entry = $list[$i];
                static::assertIsArray($entry);
                static::assertArrayHasKey('int', $entry);
                static::assertArrayHasKey('string', $entry);
                static::assertArrayHasKey('bool', $entry);
            }
            $count++;
        }

        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_list_of_structures_nullable_column(ParquetEngine $engine): void
    {
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/lists.parquet');

        static::assertNull($file->metadata()->schema()->get('list_of_structs_nullable')->type());
        static::assertEquals(
            'LIST',
            $file->metadata()->schema()->get('list_of_structs_nullable')->logicalType()?->name(),
        );

        $count = 0;

        foreach ($file->values(['list_of_structs_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                // @mago-ignore analysis:mixed-assignment
                /** @var array<array-key, mixed> $list */
                $list = $row['list_of_structs_nullable'];
                static::assertIsArray($list);

                foreach ($list as $rowList) {
                    /** @var array<array-key, mixed> $entry */
                    $entry = $rowList;
                    static::assertIsArray($entry);
                    static::assertIsInt($entry['id']);
                    static::assertIsString($entry['name']);
                }
            } else {
                static::assertNull($row['list_of_structs_nullable']);
            }
            $count++;
        }

        static::assertSame(100, $count);
        static::assertSame($file->metadata()->rowsNumber(), $count);
    }
}
