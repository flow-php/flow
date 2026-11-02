<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetExtractor;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Adapter\Parquet\Tests\Context\PushdownParityContext;
use Flow\ETL\API\DataFrame;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Planning\Optimizer\Rule\PushFilterIntoSource;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Writer;
use Flow\Parquet\Writer\StatisticsLevel;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function range;
use function sprintf;

use const NAN;

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
            'absent column is not null' => [ref('extra')->isNotNull(), true],
            'a negated comparison on a NaN column' => [not(ref('score')->lessThanEqual(lit(20.0))), false],
            'numeric-looking strings' => [ref('name')->lessThan(lit('10')), false],
            'decimal equality with a literal a float cannot hold' => [ref('tenth')->equals(lit(15.1)), true],
            'a date literal on a timestamp column' => [
                ref('ts_us')->greaterThanEqual(lit(new DateTimeImmutable('2024-01-02'))),
                true,
            ],
            'a date literal in a non-UTC zone on a timestamp column' => [
                ref('ts_us')->greaterThanEqual(lit(
                    new DateTimeImmutable('2024-01-02', new DateTimeZone('Europe/Warsaw')),
                )),
                true,
            ],
            'an int literal on a float column' => [ref('score')->greaterThan(lit(20)), false],
            'a greater-than on a NaN column' => [ref('score')->greaterThan(lit(20.0)), false],
            'a between on a NaN column' => [ref('score')->between(lit(20.0), lit(100.0)), false],
            'a greater-than above the max of row groups holding NaN' => [ref('score')->greaterThan(lit(1000.0)), false],
            'a negated greater-than on a NaN column' => [not(ref('score')->greaterThan(lit(20.0))), false],
            'conjuncts kept in PHP before pushed ones' => [
                ref('id')
                    ->mod(2)
                    ->greaterThanEqual(lit(0))
                    ->and(ref('id')->mod(3)->greaterThanEqual(lit(0)))
                    ->and(ref('id')->greaterThan(lit(3)))
                    ->and(ref('name')->notEquals(lit(''))),
                false,
            ],
            'a float literal on an int column' => [ref('id')->greaterThan(lit(5.0)), false],
            'an is in holding null' => [ref('id')->isIn(lit([1, null, 7])), true],
            'a doubly negated is in holding null' => [not(not(ref('id')->isIn(lit([1, null])))), true],
            'a negated equality on a column absent from a file, or an equality' => [
                not(ref('extra')->equals(lit('x')))->or(ref('id')->equals(lit(35))),
                false,
            ],
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

    #[DataProvider('engines')]
    public function test_a_date_literal_on_a_timestamp_column_read_in_a_non_utc_zone_returns_what_the_unpushed_one_returns(?ParquetEngine $engine): void
    {
        $memory = memory_filesystem();
        $writer = new Writer(engine: new PhpParquetEngine());
        $writer->openForStream(
            $memory->writeTo(path('memory://zoned/a.parquet')),
            Schema::with(FlatColumn::int64('id'), FlatColumn::dateTime('at')),
        );
        $writer->writeBatch(array_map(
            static fn(int $id): array => [
                'id' => $id,
                'at' => (new DateTimeImmutable('2024-01-01 20:00:00'))->modify(sprintf('+%d hours', $id)),
            ],
            range(0, 8),
        ));
        $writer->close();
        $read = static fn(DataFrame $frame): array => $frame
            ->filter(ref('at')->greaterThanEqual(lit(new DateTimeImmutable('2024-01-02'))))
            ->fetch()
            ->reduceToArray('id');
        $extractor = static fn(): ParquetExtractor => from_parquet(
            path('memory://zoned/a.parquet'),
            engine: $engine,
            filesystem: $memory,
        )->withSchema(schema(
            int_schema('id', nullable: true),
            datetime_schema('at', nullable: true, zone: 'Europe/Warsaw'),
        ));

        $plain = $read(
            df(config_builder()->optimizer(Optimizer::default()->without(PushFilterIntoSource::class)))
                ->read($extractor()),
        );

        static::assertNotSame([], $plain);
        static::assertSame($plain, $read(df()->read($extractor())));
    }

    #[DataProvider('engines')]
    public function test_a_greater_than_on_a_float_column_free_of_nan_stays_pushed(?ParquetEngine $engine): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::scores($memory, 'memory://scores/a.parquet', [
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            6.0,
            7.0,
            8.0,
            9.0,
        ]);
        $pushedFilesystem = new CountingFilesystem($memory);
        $plainFilesystem = new CountingFilesystem($memory);
        $frame = static fn(?Optimizer $optimizer, CountingFilesystem $filesystem): DataFrame => df(
            $optimizer === null ? config_builder() : config_builder()->optimizer($optimizer),
        )
            ->read(from_parquet(path('memory://scores/a.parquet'), engine: $engine, filesystem: $filesystem))
            ->filter(ref('score')->greaterThan(lit(6.5)));

        $plain = $frame(Optimizer::default()->without(PushFilterIntoSource::class), $plainFilesystem)->fetch();
        $pushed = $frame(null, $pushedFilesystem);

        static::assertStringEndsWith('Filter: score > 6.5', $pushed->explain()->toString());
        static::assertSame([6, 7, 8], $plain->reduceToArray('id'));
        static::assertSame($plain->reduceToArray('id'), $pushed->fetch()->reduceToArray('id'));
        static::assertLessThan(
            ParquetFilesContext::bytesRead($plainFilesystem),
            ParquetFilesContext::bytesRead($pushedFilesystem),
        );
    }

    #[DataProvider('engines')]
    public function test_a_greater_than_on_a_float_column_without_statistics_keeps_its_nan_rows(?ParquetEngine $engine): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::scores(
            $memory,
            'memory://scores/a.parquet',
            [1.0, NAN, 3.0, 4.0, 5.0, 6.0],
            StatisticsLevel::NONE,
        );

        static::assertSame(
            [1, 4, 5],
            df()
                ->read(from_parquet(path('memory://scores/a.parquet'), engine: $engine, filesystem: $memory))
                ->filter(ref('score')->greaterThan(lit(4.5)))
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_every_conjunct_the_source_answers_is_pushed_past_the_ones_kept_in_php(): void
    {
        $memory = memory_filesystem();
        PushdownParityContext::write($memory);

        static::assertStringEndsWith(
            "Condition: Mod(id, 2) >= 0 AND Mod(id, 3) >= 0\n   └─ #1 Read\n         Extractor: ParquetExtractor\n"
            . "         Source: memory://parity/*.parquet\n         Filter: id > 3\n         Filter: name != ''\n"
            . '         Filter: uuid IS NOT NULL',
            df()
                ->read(from_parquet(path(PushdownParityContext::GLOB), filesystem: $memory)->unionByName())
                ->filter(
                    ref('id')
                        ->mod(2)
                        ->greaterThanEqual(lit(0))
                        ->and(ref('id')->mod(3)->greaterThanEqual(lit(0)))
                        ->and(ref('id')->greaterThan(lit(3)))
                        ->and(ref('name')->notEquals(lit('')))
                        ->and(ref('uuid')->isNotNull()),
                )
                ->explain()
                ->toString(),
        );
    }

    public function test_a_kept_conjunct_no_longer_throws_on_rows_a_pushed_conjunct_removes(): void
    {
        $memory = memory_filesystem();
        PushdownParityContext::write($memory);
        $filter = ref('extra')->upper()->equals(lit('X'))->and(ref('extra')->isNotNull());
        $read = static fn(): ParquetExtractor => from_parquet(
            path(PushdownParityContext::GLOB),
            filesystem: $memory,
        )->unionByName();

        static::assertCount(30, df()->read($read())->filter($filter)->fetch());

        $this->expectException(EvaluationException::class);
        df(config_builder()->optimizer(Optimizer::default()->without(PushFilterIntoSource::class)))
            ->read($read())
            ->filter($filter)
            ->fetch();
    }

    /**
     * @return Generator<string, array{?ParquetEngine, ScalarFunction, int, ?int, list<int>}>
     */
    public static function windows(): Generator
    {
        foreach (self::engines() as $engine => [$parquetEngine]) {
            yield "{$engine}, an offset inside the first file" => [
                $parquetEngine,
                ref('id')->greaterThanEqual(lit(5)),
                3,
                4,
                [8, 9, 10, 11],
            ];
            yield "{$engine}, an offset into the second file" => [
                $parquetEngine,
                ref('id')->greaterThanEqual(lit(5)),
                27,
                5,
                [32, 33, 34, 35, 36],
            ];
            yield "{$engine}, an offset past every match" => [
                $parquetEngine,
                ref('id')->greaterThanEqual(lit(5)),
                100,
                null,
                [],
            ];
            yield "{$engine}, an offset inside a residual file" => [
                $parquetEngine,
                ref('score')->greaterThan(lit(20.0)),
                14,
                4,
                [18, 19, 20, 21],
            ];
            yield "{$engine}, an offset from a residual file into an exact one" => [
                $parquetEngine,
                ref('score')->greaterThan(lit(20.0)),
                34,
                4,
                [38, 39],
            ];
        }
    }

    /**
     * @param int<0, max> $offset
     * @param list<int> $ids
     */
    #[DataProvider('windows')]
    public function test_an_offset_and_a_limit_beside_a_pushed_filter_return_what_the_unpushed_read_returns(
        ?ParquetEngine $engine,
        ScalarFunction $predicate,
        int $offset,
        ?int $limit,
        array $ids,
    ): void {
        $memory = memory_filesystem();
        PushdownParityContext::write($memory);
        $read = static fn(): ParquetExtractor => from_parquet(
            path(PushdownParityContext::GLOB),
            engine: $engine,
            filesystem: $memory,
        )->unionByName();

        static::assertSame(
            $ids,
            df()->read($read())->filter($predicate)->offset($offset)->limit($limit)->fetch()->reduceToArray('id'),
        );
        static::assertSame(
            $ids,
            df(config_builder()->optimizer(Optimizer::default()->without(PushFilterIntoSource::class)))
                ->read($read())
                ->filter($predicate)
                ->offset($offset)
                ->limit($limit)
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    #[DataProvider('engines')]
    public function test_an_offset_and_a_limit_beside_a_partition_and_a_body_conjunct_are_pushed_and_return_what_the_unpushed_read_returns(?ParquetEngine $engine): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::rowGroups($memory, 'memory://part/year=2023/a.parquet', 9, 3);
        ParquetFilesContext::rowGroups($memory, 'memory://part/year=2024/b.parquet', 9, 3);
        ParquetFilesContext::rowGroups($memory, 'memory://part/year=2025/c.parquet', 9, 3);
        $read = static fn(): ParquetExtractor => from_parquet(
            path('memory://part/**/*.parquet'),
            engine: $engine,
            filesystem: $memory,
        );
        $filter = ref('year')->notEquals(lit('2024'))->and(ref('id')->greaterThan(lit(2)));
        $expected = [['id' => 8, 'year' => '2023'], ['id' => 3, 'year' => '2025'], ['id' => 4, 'year' => '2025']];

        static::assertSame(
            $expected,
            df()->read($read())->filter($filter)->offset(5)->limit(3)->select('id', 'year')->fetch()->toArray(),
        );
        static::assertSame(
            $expected,
            df(config_builder()->optimizer(Optimizer::default()->without(PushFilterIntoSource::class)))
                ->read($read())
                ->filter($filter)
                ->offset(5)
                ->limit(3)
                ->select('id', 'year')
                ->fetch()
                ->toArray(),
        );
    }

    /**
     * @return Generator<string, array{?ParquetEngine, int<0, max>, ?int}>
     */
    public static function windows_beside_a_partition_only_filter(): Generator
    {
        foreach (self::engines() as $engine => [$parquetEngine]) {
            yield "{$engine}, an offset" => [$parquetEngine, 3, null];
            yield "{$engine}, an offset and a limit" => [$parquetEngine, 3, 2];
            yield "{$engine}, an offset past the first file" => [$parquetEngine, 11, null];
        }
    }

    /**
     * @param int<0, max> $offset
     */
    #[DataProvider('windows_beside_a_partition_only_filter')]
    public function test_a_window_beside_a_partition_only_filter_returns_what_the_unpushed_read_returns(
        ?ParquetEngine $engine,
        int $offset,
        ?int $limit,
    ): void {
        $memory = memory_filesystem();
        ParquetFilesContext::rowGroups($memory, 'memory://part/year=2023/a.parquet', 9, 3);
        ParquetFilesContext::rowGroups($memory, 'memory://part/year=2024/b.parquet', 9, 3);
        ParquetFilesContext::rowGroups($memory, 'memory://part/year=2025/c.parquet', 9, 3);
        $read = static fn(): ParquetExtractor => from_parquet(
            path('memory://part/**/*.parquet'),
            engine: $engine,
            filesystem: $memory,
        );
        $unpushed = df(config_builder()->optimizer(Optimizer::default()->without(PushFilterIntoSource::class)));
        $filter = ref('year')->notEquals(lit('2024'));

        static::assertSame(
            $unpushed
                ->read($read())
                ->filter($filter)
                ->offset($offset)
                ->limit($limit)
                ->select('id', 'year')
                ->fetch()
                ->toArray(),
            df()
                ->read($read())
                ->filter($filter)
                ->offset($offset)
                ->limit($limit)
                ->select('id', 'year')
                ->fetch()
                ->toArray(),
        );
        static::assertSame(
            $unpushed->read($read())->filter($filter)->offset($offset)->count(),
            df()->read($read())->filter($filter)->offset($offset)->count(),
        );
    }

    /**
     * @return Generator<string, array{ScalarFunction, list<array{id: int, year: ?string}>}>
     */
    public static function partition_filters_on_a_mixed_depth_glob(): Generator
    {
        yield 'year IS NULL' => [
            ref('year')->isNull(),
            [['id' => 0, 'year' => null], ['id' => 1, 'year' => null], ['id' => 2, 'year' => null]],
        ];
        yield "year = '2023'" => [
            ref('year')->equals(lit('2023')),
            [['id' => 0, 'year' => '2023'], ['id' => 1, 'year' => '2023'], ['id' => 2, 'year' => '2023']],
        ];
    }

    /**
     * @param list<array{id: int, year: ?string}> $rows
     */
    #[DataProvider('partition_filters_on_a_mixed_depth_glob')]
    public function test_a_partition_filter_on_a_glob_where_a_file_lacks_the_partition_reads_it_as_null(
        ScalarFunction $filter,
        array $rows,
    ): void {
        $memory = memory_filesystem();
        ParquetFilesContext::rowGroups($memory, 'memory://part/year=2023/a.parquet', 3, 3);
        ParquetFilesContext::rowGroups($memory, 'memory://part/b.parquet', 3, 3);
        $read = static fn(): ParquetExtractor => from_parquet(path('memory://part/**/*.parquet'), filesystem: $memory);

        static::assertSame($rows, df()->read($read())->filter($filter)->select('id', 'year')->fetch()->toArray());
        static::assertSame(
            $rows,
            df(config_builder()->optimizer(Optimizer::default()->without(PushFilterIntoSource::class)))
                ->read($read())
                ->filter($filter)
                ->select('id', 'year')
                ->fetch()
                ->toArray(),
        );
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

    public function test_a_conjunct_the_parquet_filter_translates_leaves_its_filter_node(): void
    {
        $memory = memory_filesystem();
        PushdownParityContext::write($memory);

        static::assertStringEndsWith(
            "└─ #1 Read\n      Extractor: ParquetExtractor\n      Source: memory://parity/*.parquet\n      Filter: id > 1",
            df()
                ->read(from_parquet(path(PushdownParityContext::GLOB), filesystem: $memory)->unionByName())
                ->filter(ref('id')->greaterThan(lit(1)))
                ->explain()
                ->toString(),
        );
    }

    public function test_a_conjunct_the_parquet_filter_cannot_translate_stays_a_filter_node(): void
    {
        $memory = memory_filesystem();
        PushdownParityContext::write($memory);

        static::assertStringEndsWith(
            "└─ #2 Filter\n   │  Condition: EndsWith(name, '1')\n   └─ #1 Read\n         Extractor: ParquetExtractor\n"
            . '         Source: memory://parity/*.parquet',
            df()
                ->read(from_parquet(path(PushdownParityContext::GLOB), filesystem: $memory)->unionByName())
                ->filter(ref('name')->endsWith(lit('1')))
                ->explain()
                ->toString(),
        );
    }
}
