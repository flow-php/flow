<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Reader;
use PHPUnit\Framework\TestCase;

final class ListsReadingTest extends TestCase
{
    public function test_reading_list_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/lists.parquet');

        self::assertNull($file->metadata()->schema()->get('list')->type());
        self::assertEquals('LIST', $file->metadata()->schema()->get('list')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list']) as $row) {
            self::assertContainsOnly('int', $row['list']);
            self::assertCount(3, $row['list']);
            $count++;
        }

        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_list_column_with_limit() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/lists.parquet');

        self::assertNull($file->metadata()->schema()->get('list')->type());
        self::assertEquals('LIST', $file->metadata()->schema()->get('list')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list'], $limit = 50) as $row) {
            self::assertContainsOnly('int', $row['list']);
            self::assertCount(3, $row['list']);
            $count++;
        }

        self::assertSame($limit, $count);
    }

    public function test_reading_list_nested_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/lists.parquet');

        self::assertNull($file->metadata()->schema()->get('list_nested')->type());
        self::assertEquals('LIST', $file->metadata()->schema()->get('list_nested')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list_nested']) as $row) {
            self::assertIsArray($row['list_nested']);
            self::assertIsList($row['list_nested']);
            self::assertIsArray($row['list_nested'][0]);
            self::assertIsList($row['list_nested'][0]);
            self::assertIsArray($row['list_nested'][0][0]);

            $count++;
        }
        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_list_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/lists.parquet');

        self::assertNull($file->metadata()->schema()->get('list_nullable')->type());
        self::assertEquals('LIST', $file->metadata()->schema()->get('list_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                self::assertContainsOnly('int', $row['list_nullable']);
                self::assertCount(3, $row['list_nullable']);
            } else {
                self::assertNull($row['list_nullable']);
            }
            $count++;
        }

        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_list_of_structures_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/lists.parquet');

        self::assertNull($file->metadata()->schema()->get('list_mixed_types')->type());
        self::assertEquals('LIST', $file->metadata()->schema()->get('list_mixed_types')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list_mixed_types']) as $row) {
            self::assertIsArray($row['list_mixed_types']);
            self::assertCount(4, $row['list_mixed_types']);
            self::assertArrayHasKey('int', $row['list_mixed_types'][0]);
            self::assertArrayHasKey('string', $row['list_mixed_types'][0]);
            self::assertArrayHasKey('bool', $row['list_mixed_types'][0]);
            self::assertArrayHasKey('int', $row['list_mixed_types'][1]);
            self::assertArrayHasKey('string', $row['list_mixed_types'][1]);
            self::assertArrayHasKey('bool', $row['list_mixed_types'][1]);
            self::assertArrayHasKey('int', $row['list_mixed_types'][2]);
            self::assertArrayHasKey('string', $row['list_mixed_types'][2]);
            self::assertArrayHasKey('bool', $row['list_mixed_types'][2]);
            self::assertArrayHasKey('int', $row['list_mixed_types'][3]);
            self::assertArrayHasKey('string', $row['list_mixed_types'][3]);
            self::assertArrayHasKey('bool', $row['list_mixed_types'][3]);
            $count++;
        }

        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_list_of_structures_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/lists.parquet');

        self::assertNull($file->metadata()->schema()->get('list_of_structs_nullable')->type());
        self::assertEquals('LIST', $file->metadata()->schema()->get('list_of_structs_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list_of_structs_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                self::assertIsArray($row['list_of_structs_nullable']);

                foreach ($row['list_of_structs_nullable'] as $rowList) {
                    self::assertIsInt($rowList['id']);
                    self::assertIsString($rowList['name']);
                }
            } else {
                self::assertNull($row['list_of_structs_nullable']);
            }
            $count++;
        }

        self::assertSame(100, $count);
        self::assertSame($file->metadata()->rowsNumber(), $count);
    }
}
