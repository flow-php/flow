<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\Tests\Context\StatisticsParityRows;
use Flow\Parquet\Tests\Mother\ParquetEngineMother;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function hex2bin;
use function str_repeat;
use function var_export;

#[Group('native-extension')]
final class StatisticsParityTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('arrow')) {
            self::markTestSkipped('arrow is not loaded');
        }
    }

    public function test_both_engines_write_the_expected_statistics(): void
    {
        $expected = [
            'digits' => [null, null, '10', '9', 0, null, true, true, ColumnOrder::TypeDefined],
            'long' => [
                null,
                null,
                str_repeat('a', 64),
                str_repeat('c', 63) . 'd',
                0,
                null,
                false,
                false,
                ColumnOrder::TypeDefined,
            ],
            'double' => [null, null, -0.0, 0.0, 0, 1, true, true, ColumnOrder::Ieee754TotalOrder],
            'float' => [null, null, -2.0, 1.5, 0, 1, true, true, ColumnOrder::Ieee754TotalOrder],
            'decimal' => [-350, 125, -350, 125, 1, null, true, true, ColumnOrder::TypeDefined],
            'int32' => [-7, 7, -7, 7, 1, null, true, true, ColumnOrder::TypeDefined],
            'int64' => [1, 3, 1, 3, 1, null, true, true, ColumnOrder::TypeDefined],
            'uint64' => [null, null, 0, -1, 0, null, true, true, ColumnOrder::TypeDefined],
            'boolean' => [null, null, false, true, 1, null, true, true, ColumnOrder::TypeDefined],
            'date' => [10956, 19724, 10956, 19724, 1, null, true, true, ColumnOrder::TypeDefined],
            'uuid' => [
                null,
                null,
                (string) hex2bin('00000000000000000000000000000002'),
                (string) hex2bin('ffffffff000000000000000000000001'),
                1,
                null,
                true,
                true,
                ColumnOrder::TypeDefined,
            ],
            'decimal30' => [
                (string) hex2bin('fffffffffffffffffffffffea2'),
                (string) hex2bin('0000000000112210f47de98120'),
                (string) hex2bin('fffffffffffffffffffffffea2'),
                (string) hex2bin('0000000000112210f47de98120'),
                1,
                null,
                true,
                true,
                ColumnOrder::TypeDefined,
            ],
            'binary' => [null, null, "\x00\xff", "\x80", 0, null, true, true, ColumnOrder::TypeDefined],
            'timestamp' => [
                -1_000_000,
                1_704_164_645_123_456,
                -1_000_000,
                1_704_164_645_123_456,
                1,
                null,
                true,
                true,
                ColumnOrder::TypeDefined,
            ],
            'enum' => [null, null, 'BLUE', 'RED', 0, null, true, true, ColumnOrder::TypeDefined],
        ];

        static::assertSame(
            var_export($expected, true),
            var_export(StatisticsParityRows::statistics(ParquetEngineMother::create(PhpParquetEngine::class)), true),
        );
        static::assertSame(
            var_export($expected, true),
            var_export(StatisticsParityRows::statistics(ParquetEngineMother::create(RustParquetEngine::class)), true),
        );
    }
}
