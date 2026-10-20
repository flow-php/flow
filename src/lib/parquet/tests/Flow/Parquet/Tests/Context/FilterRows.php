<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use DateTimeImmutable;
use Flow\Parquet\BloomFilter\BloomFilterOptions;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Mother\ParquetEngineMother;
use Flow\Parquet\Writer;

use function bcsub;
use function file_get_contents;
use function intdiv;
use function is_array;
use function preg_match;
use function sprintf;

use const NAN;

/**
 * The rows of the Filters files: four row groups of `$rowsPerGroup` rows, row i has id i. The PHP and arrow files hold
 * groups of 10; the DuckDB one groups of 2048, the smallest DuckDB writes.
 */
final class FilterRows
{
    public const string FIXTURES = __DIR__ . '/../Integration/Engine/Fixtures/Filters/';

    public const int DUCKDB_BLOOM_ROWS = 8192;

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
     * The rows of the bloom files: even ids only, so an odd id lies inside every group's min/max and
     * only a bloom filter proves it absent; key is 'k' followed by id.
     *
     * @return list<array{id: int, key: string}>
     */
    public static function bloomRows(int $rows = 4 * self::ROWS_PER_GROUP): array
    {
        $bloomRows = [];

        for ($i = 0; $i < $rows; $i++) {
            $bloomRows[] = ['id' => $i * 2, 'key' => 'k' . ($i * 2)];
        }

        return $bloomRows;
    }

    public static function bloomSchema(): Schema
    {
        return Schema::with(FlatColumn::int64('id'), FlatColumn::string('key'));
    }

    /**
     * The rows of the pages files (40 rows, one row group, pages of 5 rows): `address` holds a list
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

    /**
     * The bytes of a Filters file: `row_groups`, `pages` or `bloom` with `_php` or `_arrow`, written in memory by that
     * writer, or one of FIXTURES by its name.
     *
     * @return non-empty-string
     */
    public static function bytes(string $fixture): string
    {
        $written = [];

        if (preg_match('/^(row_groups|pages|bloom)_(php|arrow)$/', $fixture, $written) !== 1) {
            $bytes = file_get_contents(self::FIXTURES . $fixture . '.parquet');

            return $bytes === false || $bytes === ''
                ? throw new InvalidArgumentException('No fixture ' . $fixture)
                : $bytes;
        }

        $options = match ($written[1]) {
            'pages' => Options::default()->set(Option::PAGE_SIZE_BYTES, 1)->set(
                Option::PAGE_SIZE_CHECK_INTERVAL,
                self::ROWS_PER_PAGE,
            )->set(Option::ARROW_WRITE_BATCH_SIZE, self::ROWS_PER_PAGE),
            default => Options::default()
                ->set(Option::ROW_GROUP_SIZE_BYTES, 1)
                ->set(Option::ROW_GROUP_SIZE_CHECK_INTERVAL, self::ROWS_PER_GROUP)
                ->set(Option::PAGE_SIZE_BYTES, 1)
                ->set(Option::PAGE_SIZE_CHECK_INTERVAL, self::ROWS_PER_GROUP)
                ->set(Option::ARROW_WRITE_BATCH_SIZE, self::ROWS_PER_GROUP)
                ->set(
                    Option::COLUMNS_BLOOM_FILTERS,
                    $written[1] === 'bloom'
                        ? ['id' => new BloomFilterOptions(), 'key' => new BloomFilterOptions()]
                        : null,
                ),
        };

        return MemoryParquetFile::written(
            new Writer(
                options: $options,
                engine: $written[2] === 'php' ? new PhpParquetEngine() : new RustParquetEngine($options),
            ),
            match ($written[1]) {
                'pages' => self::pageSchema(),
                'bloom' => self::bloomSchema(),
                default => self::schema(),
            },
            match ($written[1]) {
                'pages' => self::pageRows(),
                'bloom' => self::bloomRows(),
                default => self::rows(),
            },
        );
    }

    /**
     * A Filters file (see bytes()) read by $engine.
     *
     * @param class-string<ParquetEngine> $engine
     *
     * @return ParquetFile<ParquetFileReader>
     */
    public static function file(string $fixture, string $engine): ParquetFile
    {
        return MemoryParquetFile::read(new Reader(engine: ParquetEngineMother::create($engine)), self::bytes($fixture));
    }
}
