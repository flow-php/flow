<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use DateTimeImmutable;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function bcsub;
use function intdiv;
use function is_array;
use function sprintf;

use const NAN;

/**
 * The rows of Fixtures/Filters: four row groups of `$rowsPerGroup` rows, row i has id i. The PHP and arrow files hold
 * groups of 10; the DuckDB one groups of 2048, the smallest DuckDB writes.
 */
final class FilterRows
{
    public const int DUCKDB_ROWS_PER_GROUP = 2048;

    public const int ROWS_PER_GROUP = 10;

    public const int ROWS_PER_PAGE = 5;

    /**
     * @return list<array<string, mixed>>
     */
    public static function rows(int $rowsPerGroup = self::ROWS_PER_GROUP): array
    {
        $rows = [];

        for ($i = 0; $i < (4 * $rowsPerGroup); $i++) {
            $group = intdiv($i, $rowsPerGroup);
            $rows[] = [
                'id' => $i,
                'label' => match ($group) {
                    0 => '9',
                    1 => '10',
                    2 => '100',
                    default => ['9', '10', '100'][$i % 3],
                },
                'score' => match (true) {
                    $i === $rowsPerGroup => -0.0,
                    $i === ($rowsPerGroup + 1) || $group === 2 => NAN,
                    default => (float) $i,
                },
                'maybe' => $group === 0 || $group === 1 && ($i % 2) === 1 ? null : $i,
                'day' => new DateTimeImmutable(sprintf('2024-01-01 +%d days UTC', $i)),
                'ts' => new DateTimeImmutable(sprintf('2024-01-01 00:00:00.123456 +%d hours UTC', $i)),
                'amount' => $i + 0.25,
                'address' => $group === 3 && ($i % 5) === 0 ? null : ['city' => 'c' . $group],
                'big' => $group >= 2
                    ? bcsub('18446744073709551615', (string) ((4 * $rowsPerGroup) - 1 - $i))
                    : (string) $i,
                'uid' => sprintf('%08x-0000-4000-8000-%012x', $group, $i),
                'flag' => match ($group) {
                    0 => false,
                    1 => true,
                    default => ($i % 2) === 0,
                },
                'ratio' => $i + 0.5,
                'word' => match ($group) {
                    0 => 'ąbc',
                    1 => 'źdźbło',
                    2 => 'zebra',
                    default => ['ąbc', 'źdźbło', 'zebra'][$i % 3],
                },
            ];
        }

        return $rows;
    }

    public static function schema(): Schema
    {
        return Schema::with(
            FlatColumn::int64('id'),
            FlatColumn::string('label'),
            FlatColumn::double('score'),
            FlatColumn::int64('maybe'),
            FlatColumn::date('day'),
            FlatColumn::dateTime('ts'),
            FlatColumn::decimal('amount', 10, 2),
            NestedColumn::struct('address', [FlatColumn::string('city')]),
            new FlatColumn('big', PhysicalType::INT64, logicalType: LogicalType::integer(64, false)),
            FlatColumn::uuid('uid'),
            FlatColumn::boolean('flag'),
            FlatColumn::float('ratio'),
            FlatColumn::string('word'),
        );
    }

    /**
     * The rows of Fixtures/Filters/pages_*.parquet (40 rows, one row group, pages of 5 rows): `address` holds a list
     * `codes` beside `city`, id % 4 copies of id, empty when id % 4 is 0.
     *
     * @return list<array<string, mixed>>
     */
    public static function pageRows(): array
    {
        $rows = [];

        foreach (self::rows() as $row) {
            /** @var int $id */
            $id = $row['id'];
            $codes = [];

            for ($code = 0; $code < ($id % 4); $code++) {
                $codes[] = $id;
            }

            if (is_array($row['address'])) {
                $row['address']['codes'] = $codes;
            }

            $rows[] = $row;
        }

        return $rows;
    }

    public static function pageSchema(): Schema
    {
        return Schema::with(
            FlatColumn::int64('id'),
            FlatColumn::string('label'),
            FlatColumn::double('score'),
            FlatColumn::int64('maybe'),
            FlatColumn::date('day'),
            FlatColumn::dateTime('ts'),
            FlatColumn::decimal('amount', 10, 2),
            NestedColumn::struct('address', [
                FlatColumn::string('city'),
                NestedColumn::list('codes', ListElement::int64()),
            ]),
            new FlatColumn('big', PhysicalType::INT64, logicalType: LogicalType::integer(64, false)),
            FlatColumn::uuid('uid'),
            FlatColumn::boolean('flag'),
            FlatColumn::float('ratio'),
            FlatColumn::string('word'),
        );
    }
}
