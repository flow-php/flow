<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Reader;
use PHPUnit\Framework\TestCase;

final class ListsReadingTest extends TestCase
{
    public function test_reading_list_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/../../Fixtures/lists.parquet');

        $this->assertNull($file->metadata()->schema()->get('list')->type());
        $this->assertEquals('LIST', $file->metadata()->schema()->get('list')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list']) as $row) {
            $this->assertContainsOnly('int', $row['list']);
            $this->assertCount(3, $row['list']);
            $count++;
        }

        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_list_column_with_limit() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/../../Fixtures/lists.parquet');

        $this->assertNull($file->metadata()->schema()->get('list')->type());
        $this->assertEquals('LIST', $file->metadata()->schema()->get('list')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list'], $limit = 50) as $row) {
            $this->assertContainsOnly('int', $row['list']);
            $this->assertCount(3, $row['list']);
            $count++;
        }

        $this->assertSame($limit, $count);
    }

    public function test_reading_list_nested_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/../../Fixtures/lists.parquet');

        $this->assertNull($file->metadata()->schema()->get('list_nested')->type());
        $this->assertEquals('LIST', $file->metadata()->schema()->get('list_nested')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list_nested']) as $row) {
            $this->assertIsArray($row['list_nested']);
            $this->assertIsList($row['list_nested']);
            $this->assertIsArray($row['list_nested'][0]);
            $this->assertIsList($row['list_nested'][0]);
            $this->assertIsArray($row['list_nested'][0][0]);

            $count++;
        }
        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_list_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/../../Fixtures/lists.parquet');

        $this->assertNull($file->metadata()->schema()->get('list_nullable')->type());
        $this->assertEquals('LIST', $file->metadata()->schema()->get('list_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                $this->assertContainsOnly('int', $row['list_nullable']);
                $this->assertCount(3, $row['list_nullable']);
            } else {
                $this->assertNull($row['list_nullable']);
            }
            $count++;
        }

        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_list_of_structures_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/../../Fixtures/lists.parquet');

        $this->assertNull($file->metadata()->schema()->get('list_mixed_types')->type());
        $this->assertEquals('LIST', $file->metadata()->schema()->get('list_mixed_types')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list_mixed_types']) as $row) {
            $this->assertIsArray($row['list_mixed_types']);
            $this->assertCount(4, $row['list_mixed_types']);
            $this->assertArrayHasKey('int', $row['list_mixed_types'][0]);
            $this->assertArrayHasKey('string', $row['list_mixed_types'][0]);
            $this->assertArrayHasKey('bool', $row['list_mixed_types'][0]);
            $this->assertArrayHasKey('int', $row['list_mixed_types'][1]);
            $this->assertArrayHasKey('string', $row['list_mixed_types'][1]);
            $this->assertArrayHasKey('bool', $row['list_mixed_types'][1]);
            $this->assertArrayHasKey('int', $row['list_mixed_types'][2]);
            $this->assertArrayHasKey('string', $row['list_mixed_types'][2]);
            $this->assertArrayHasKey('bool', $row['list_mixed_types'][2]);
            $this->assertArrayHasKey('int', $row['list_mixed_types'][3]);
            $this->assertArrayHasKey('string', $row['list_mixed_types'][3]);
            $this->assertArrayHasKey('bool', $row['list_mixed_types'][3]);
            $count++;
        }

        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }

    public function test_reading_list_of_structures_nullable_column() : void
    {
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/../../Fixtures/lists.parquet');

        $this->assertNull($file->metadata()->schema()->get('list_of_structs_nullable')->type());
        $this->assertEquals('LIST', $file->metadata()->schema()->get('list_of_structs_nullable')->logicalType()->name());

        $count = 0;

        foreach ($file->values(['list_of_structs_nullable']) as $rowIndex => $row) {
            if ($rowIndex % 2 === 0) {
                $this->assertIsArray($row['list_of_structs_nullable']);

                foreach ($row['list_of_structs_nullable'] as $rowList) {
                    $this->assertIsInt($rowList['id']);
                    $this->assertIsString($rowList['name']);
                }
            } else {
                $this->assertNull($row['list_of_structs_nullable']);
            }
            $count++;
        }

        $this->assertSame(100, $count);
        $this->assertSame($file->metadata()->rowsNumber(), $count);
    }
}
