<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Reader;
use PHPUnit\Framework\TestCase;

final class EdgeCasesReadingTest extends TestCase
{
    public function test_nonullable_impala() : void
    {
        $path = __DIR__ . '/Fixtures/EdgeCases/nonnullable.impala.parquet';

        $reader = (new Reader())->read($path);

        self::assertEquals(
            [
                [
                    'ID' => 8,
                    'Int_Array' => [
                        -1,
                    ],
                    'int_array_array' => [
                        [-1, -2],
                        [],
                    ],
                    'Int_Map' => [
                        'k1' => -1,
                    ],
                    'int_map_array' => [
                        [],
                        ['k1' => 1],
                        [],
                        [],
                    ],
                    'nested_Struct' => [
                        'a' => -1,
                        'B' => [-1],
                        'c' => [
                            'D' => [
                                [
                                    [
                                        'e' => -1,
                                        'f' => 'nonnullable',
                                    ],
                                ],
                            ],
                        ],
                        'G' => [],
                    ],
                ],
            ],
            \iterator_to_array($reader->values())
        );
    }

    public function test_read_datapage_v2_snappy_list() : void
    {
        $this->expectExceptionMessage('Encoding DELTA_BINARY_PACKED not supported');

        $path = __DIR__ . '/Fixtures/EdgeCases/datapage_v2.snappy.parquet';

        $reader = (new Reader())->read($path);

        $rows = [];

        foreach ($reader->values() as $row) {
            $rows[] = $row;
        }

        self::assertSame(
            [
                ['emptylist' => null],
            ],
            $rows
        );
    }

    public function test_read_null_list() : void
    {
        $path = __DIR__ . '/Fixtures/EdgeCases/null_list.parquet';

        $reader = (new Reader())->read($path);

        $rows = [];

        foreach ($reader->values() as $row) {
            $rows[] = $row;
        }

        self::assertSame(
            [
                ['emptylist' => []],
            ],
            $rows
        );
    }
}
