<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\{ParquetEngine, Reader};
use PHPUnit\Framework\Attributes\DataProvider;

class EdgeCasesReadingTest extends ParquetIntegrationTestCase
{
    #[DataProvider('engine_provider')]
    public function test_nonullable_impala(ParquetEngine $engine) : void
    {
        $path = __DIR__ . '/Fixtures/EdgeCases/nonnullable.impala.parquet';

        $reader = (new Reader(engine: $engine))->read($path);

        static::assertEquals(
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
        $this->expectExceptionMessage('Encoding RLE not supported');

        $path = __DIR__ . '/Fixtures/EdgeCases/datapage_v2.snappy.parquet';

        $reader = Reader::php()->read($path);

        $rows = [];

        foreach ($reader->values() as $row) {
            $rows[] = $row;
        }

        static::assertSame(
            [
                ['emptylist' => null],
            ],
            $rows
        );
    }

    #[DataProvider('engine_provider')]
    public function test_read_null_list(ParquetEngine $engine) : void
    {
        $path = __DIR__ . '/Fixtures/EdgeCases/null_list.parquet';

        $reader = (new Reader(engine: $engine))->read($path);

        $rows = [];

        foreach ($reader->values() as $row) {
            $rows[] = $row;
        }

        static::assertSame(
            [
                ['emptylist' => []],
            ],
            $rows
        );
    }
}
