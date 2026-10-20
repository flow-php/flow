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
     * @return Generator<string, array{?ParquetEngine, ScalarFunction, bool}>
     */
    public static function predicates(): Generator
    {
        $predicates = [
            'string range' => [ref('name')->greaterThan(lit('3')), true],
            'NaN column' => [ref('score')->lessThanEqual(lit(20.0)), true],
            'decimal(9,2)' => [ref('d92')->greaterThan(lit(15.25)), true],
            'decimal(18,2)' => [ref('d182')->greaterThan(lit(15.25)), false],
            'date' => [ref('day')->greaterThanEqual(lit(new DateTimeImmutable('2024-01-21'))), true],
            'timestamp micros' => [ref('ts_us')->greaterThan(lit(new DateTimeImmutable('2024-01-01 20:30:00'))), true],
            'timestamp nanos' => [ref('ts_ns')->greaterThan(lit(new DateTimeImmutable('2024-01-01 20:30:00'))), true],
            'uuid' => [ref('uuid')->equals(lit(new Uuid('00000000-0000-4000-8000-000000000025'))), true],
            'a negated is in holding null, or an equality' => [
                not(ref('id')->isIn(lit([1, null])))->or(ref('id')->equals(lit(5))),
                false,
            ],
            'absent column equals' => [ref('extra')->equals(lit('x')), true],
            'absent column is null' => [ref('extra')->isNull(), true],
        ];

        foreach (['default engine' => null, 'php engine' => new PhpParquetEngine()] as $engineName => $engine) {
            foreach ($predicates as $name => [$predicate, $prunes]) {
                yield $engineName . ', ' . $name => [$engine, $predicate, $prunes];
            }
        }
    }

    /**
     * @param bool $prunes true when the predicate skips a row group, false when it skips none
     */
    #[DataProvider('predicates')]
    public function test_a_pushed_predicate_returns_what_the_unpushed_one_returns(
        ?ParquetEngine $engine,
        ScalarFunction $predicate,
        bool $prunes,
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
        // a pushed read adds at most the page indexes it consults to the chunks it reads
        static::assertLessThanOrEqual(
            ParquetFilesContext::bytesRead($plainFilesystem)
            + ParquetFilesContext::pageIndexBytes($memory, PushdownParityContext::CHUNKS)
            + ParquetFilesContext::offsetIndexBytes($memory, PushdownParityContext::CHUNKS),
            ParquetFilesContext::bytesRead($pushedFilesystem),
        );

        if ($prunes) {
            static::assertLessThan(
                ParquetFilesContext::bytesRead($plainFilesystem),
                ParquetFilesContext::bytesRead($pushedFilesystem),
            );
        }
    }

    /**
     * @return Generator<string, array{?ParquetEngine}>
     */
    public static function engines(): Generator
    {
        yield 'default engine' => [null];
        yield 'php engine' => [new PhpParquetEngine()];
    }

    #[DataProvider('engines')]
    public function test_a_limit_beside_a_pushed_filter_reads_fewer_bytes_than_the_unlimited_read(?ParquetEngine $engine): void
    {
        $memory = memory_filesystem();
        PushdownParityContext::write($memory);
        $limitedFilesystem = new CountingFilesystem($memory);
        $unlimitedFilesystem = new CountingFilesystem($memory);

        $limited = df()
            ->read(
                from_parquet(
                    path(PushdownParityContext::GLOB),
                    engine: $engine,
                    filesystem: $limitedFilesystem,
                )->unionByName(),
            )
            ->filter(ref('id')->lessThan(lit(35)))
            ->limit(10)
            ->fetch();
        $unlimited = df()
            ->read(
                from_parquet(
                    path(PushdownParityContext::GLOB),
                    engine: $engine,
                    filesystem: $unlimitedFilesystem,
                )->unionByName(),
            )
            ->filter(ref('id')->lessThan(lit(35)))
            ->fetch();

        static::assertCount(10, $limited);
        static::assertCount(35, $unlimited);
        static::assertLessThan(
            ParquetFilesContext::bytesRead($unlimitedFilesystem),
            ParquetFilesContext::bytesRead($limitedFilesystem),
        );
    }
}
