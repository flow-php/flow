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
        static::assertEquals('LIST', $file->metadata()->schema()->get('list')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list']) as $row) {
            static::assertContainsOnly('int', $row['list']);
            static::assertCount(3, $row['list']);
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
        static::assertEquals('LIST', $file->metadata()->schema()->get('list')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list'], $limit = 50) as $row) {
            static::assertContainsOnly('int', $row['list']);
            static::assertCount(3, $row['list']);
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
        static::assertEquals('LIST', $file->metadata()->schema()->get('list_nested')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list_nested']) as $row) {
            static::assertIsArray($row['list_nested']);
            static::assertIsList($row['list_nested']);
            static::assertIsArray($row['list_nested'][0]);
            static::assertIsList($row['list_nested'][0]);
            static::assertIsArray($row['list_nested'][0][0]);

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
        static::assertEquals('LIST', $file->metadata()->schema()->get('list_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                static::assertContainsOnly('int', $row['list_nullable']);
                static::assertCount(3, $row['list_nullable']);
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
        static::assertEquals('LIST', $file->metadata()->schema()->get('list_mixed_types')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list_mixed_types']) as $row) {
            static::assertIsArray($row['list_mixed_types']);
            static::assertCount(4, $row['list_mixed_types']);
            static::assertArrayHasKey('int', $row['list_mixed_types'][0]);
            static::assertArrayHasKey('string', $row['list_mixed_types'][0]);
            static::assertArrayHasKey('bool', $row['list_mixed_types'][0]);
            static::assertArrayHasKey('int', $row['list_mixed_types'][1]);
            static::assertArrayHasKey('string', $row['list_mixed_types'][1]);
            static::assertArrayHasKey('bool', $row['list_mixed_types'][1]);
            static::assertArrayHasKey('int', $row['list_mixed_types'][2]);
            static::assertArrayHasKey('string', $row['list_mixed_types'][2]);
            static::assertArrayHasKey('bool', $row['list_mixed_types'][2]);
            static::assertArrayHasKey('int', $row['list_mixed_types'][3]);
            static::assertArrayHasKey('string', $row['list_mixed_types'][3]);
            static::assertArrayHasKey('bool', $row['list_mixed_types'][3]);
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
            $file->metadata()->schema()->get('list_of_structs_nullable')->logicalType()->name(),
        );

        $count = 0;

        foreach ($file->values(['list_of_structs_nullable']) as $rowIndex => $row) {
            if (($rowIndex % 2) === 0) {
                static::assertIsArray($row['list_of_structs_nullable']);

                foreach ($row['list_of_structs_nullable'] as $rowList) {
                    static::assertIsInt($rowList['id']);
                    static::assertIsString($rowList['name']);
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
