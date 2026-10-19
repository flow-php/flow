<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use DateTimeImmutable;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Adapter\Parquet\Tests\Context\PushdownParityContext;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Optimizer;
use Flow\ETL\Optimizer\Rule\PushFilterIntoSource;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\ParquetEngine;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_fill_keys;
use function array_slice;
use function extension_loaded;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class PushdownParityTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{?ParquetEngine, ScalarFunction, bool, array<string, array<string, list<int>>>, array<string, array<string, list<int>>>}>
     */
    public static function predicates(): Generator
    {
        $columns = ['id', 'name', 'score', 'd92', 'd182', 'day', 'ts_us', 'ts_ns', 'uuid'];
        // a row filter loads the offset index of every leaf of every planned group on the arrow engine; the PHP engine
        // reads one only where the mask splits a group: a0, whose row 1 it drops, for every column but the predicate's
        $php = ['memory://parity/a.parquet' => array_fill_keys([...array_slice($columns, 1), 'extra'], [0])];
        $rowFiltered = [
            // the default engine is the arrow one only when the extension is loaded
            'default engine' => extension_loaded('arrow')
                ? [
                    'memory://parity/a.parquet' => array_fill_keys([...$columns, 'extra'], [0, 1, 2]),
                    'memory://parity/b.parquet' => array_fill_keys($columns, [0]),
                ]
                : $php,
            'php engine' => $php,
        ];
        $predicates = [
            'string range' => [ref('name')->greaterThan(lit('3')), true, [], []],
            'NaN column' => [ref('score')->lessThanEqual(lit(20.0)), true, [], []],
            'decimal(9,2)' => [ref('d92')->greaterThan(lit(15.25)), true, [], []],
            'decimal(18,2)' => [ref('d182')->greaterThan(lit(15.25)), false, [], []],
            'date' => [ref('day')->greaterThanEqual(lit(new DateTimeImmutable('2024-01-21'))), true, [], []],
            'timestamp micros' => [
                ref('ts_us')->greaterThan(lit(new DateTimeImmutable('2024-01-01 20:30:00'))),
                true,
                [],
                [],
            ],
            'a date literal on a timestamp column' => [
                ref('ts_us')->greaterThanEqual(lit(new DateTimeImmutable('2024-01-02'))),
                true,
                [],
                [],
            ],
            'a datetime literal above a date column' => [
                ref('day')->greaterThan(lit(new DateTimeImmutable('2024-01-20 12:00:00'))),
                true,
                [],
                [],
            ],
            'a datetime literal below a date column' => [
                ref('day')->lessThanEqual(lit(new DateTimeImmutable('2024-01-20 12:00:00'))),
                true,
                [],
                [],
            ],
            'timestamp nanos' => [
                ref('ts_ns')->greaterThan(lit(new DateTimeImmutable('2024-01-01 20:30:00'))),
                true,
                [],
                [],
            ],
            'uuid' => [ref('uuid')->equals(lit(new Uuid('00000000-0000-4000-8000-000000000025'))), true, [], []],
            'a negated is in holding null, or an equality' => [
                not(ref('id')->isIn(lit([1, null])))->or(ref('id')->equals(lit(5))),
                false,
                ['memory://parity/a.parquet' => ['id' => [0]]],
                $rowFiltered,
            ],
            'absent column equals' => [ref('extra')->equals(lit('x')), true, [], []],
            'absent column is null' => [ref('extra')->isNull(), true, [], []],
        ];

        foreach (['default engine' => null, 'php engine' => new PhpParquetEngine()] as $engineName => $engine) {
            foreach ($predicates as $name => [$predicate, $prunes, $pageIndexed, $offsetIndexed]) {
                yield $engineName . ', ' . $name => [
                    $engine,
                    $predicate,
                    $prunes,
                    $pageIndexed,
                    $offsetIndexed[$engineName] ?? [],
                ];
            }
        }
    }

    /**
     * @param bool $prunes true when the predicate skips a row group, false when it skips none
     * @param array<string, array<string, list<int>>> $pageIndexed by file, the predicate columns and the row groups whose
     *                                                page index the plan reads although it skips nothing
     * @param array<string, array<string, list<int>>> $offsetIndexed by file, the columns and the row groups whose offset
     *                                                index the row filter's read loads
     */
    #[DataProvider('predicates')]
    public function test_a_pushed_predicate_returns_what_the_unpushed_one_returns(
        ?ParquetEngine $engine,
        ScalarFunction $predicate,
        bool $prunes,
        array $pageIndexed,
        array $offsetIndexed,
    ): void {
        $memory = memory_filesystem();
        PushdownParityContext::write($memory);
        $pushedFilesystem = new CountingFilesystem($memory);
        $plainFilesystem = new CountingFilesystem($memory);

        $pushed = df()
            ->read(
                from_parquet(
                    path(PushdownParityContext::GLOB),
                    engine: $engine,
                    filesystem: $pushedFilesystem,
                )->unionByName(),
            )
            ->filter($predicate)
            ->fetch();
        $plain = df(config_builder()->optimizer(Optimizer::default()->without(PushFilterIntoSource::class)))
            ->read(
                from_parquet(
                    path(PushdownParityContext::GLOB),
                    engine: $engine,
                    filesystem: $plainFilesystem,
                )->unionByName(),
            )
            ->filter($predicate)
            ->fetch();

        static::assertNotSame([], $plain->reduceToArray('id'));
        static::assertSame($plain->reduceToArray('id'), $pushed->reduceToArray('id'));

        if ($prunes) {
            static::assertLessThan(
                ParquetFilesContext::bytesRead($plainFilesystem),
                ParquetFilesContext::bytesRead($pushedFilesystem),
            );
        } else {
            static::assertSame(
                ParquetFilesContext::bytesRead($plainFilesystem)
                + ParquetFilesContext::pageIndexBytes($memory, $pageIndexed)
                + ParquetFilesContext::offsetIndexBytes($memory, $offsetIndexed),
                ParquetFilesContext::bytesRead($pushedFilesystem),
            );
        }
    }
}
