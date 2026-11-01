<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\Reader\SkippedMatches;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Tests\Integration\IO\ParquetIntegrationTestCase;
use Flow\Parquet\Tests\Mother\ParquetEngineMother;
use Flow\Parquet\Tests\Mother\ReadPlanMother;
use Flow\Parquet\Writer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function count;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Parquet\DSL\parquet_column;
use function iterator_to_array;
use function range;

final class ReadColumnsTest extends ParquetIntegrationTestCase
{
    /**
     * @return Generator<string, array{class-string<ParquetEngine>, int, ?int, ?int, list<int>, list<list<int>>}>
     */
    public static function chunkings(): Generator
    {
        foreach (self::engine_provider() as $name => [$engine]) {
            yield "{$name}: batch size 2" => [$engine, 2, null, null, [2, 2, 1], [[1, 2], [3, 4], [5]]];
            yield "{$name}: offset 3" => [$engine, 2, null, 3, [2], [[4, 5]]];
            yield "{$name}: limit 3" => [$engine, 2, 3, null, [2, 1], [[1, 2], [3]]];
            yield "{$name}: offset 1, limit 3" => [$engine, 2, 3, 1, [2, 1], [[2, 3], [4]]];
            yield "{$name}: batch size above the row count" => [$engine, 10, null, null, [5], [[1, 2, 3, 4, 5]]];
        }
    }

    /**
     * @return Generator<string, array{class-string<ParquetEngine>, int, list<array{skipped: int}|list<int>>}>
     */
    public static function matched_offsets(): Generator
    {
        foreach (self::engine_provider() as $name => [$engine]) {
            yield "{$name}: no offset yields no skip" => [$engine, 0, [[3, 4, 5, 6, 7, 8, 9]]];
            yield "{$name}: an offset inside a row group" => [$engine, 2, [['skipped' => 2], [5, 6, 7, 8, 9]]];
            yield "{$name}: an offset across row groups" => [$engine, 4, [['skipped' => 4], [7, 8, 9]]];
            yield "{$name}: an offset covering the file" => [$engine, 100, [['skipped' => 7]]];
        }
    }

    /**
     * @param class-string<ParquetEngine> $engineClass
     * @param list<array{skipped: int}|list<int>> $expected
     */
    #[DataProvider('matched_offsets')]
    public function test_an_offset_counts_the_rows_the_row_filter_keeps_and_is_announced_before_the_first_chunk(
        string $engineClass,
        int $offset,
        array $expected,
    ): void {
        $path = TestParquetFile::path($this);
        $options = Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)->set(
            Option::ROW_GROUP_SIZE_CHECK_INTERVAL,
            3,
        );
        $schema = Schema::with(FlatColumn::int32('id', Repetition::REQUIRED));
        (new Writer(options: $options, engine: new PhpParquetEngine(options: $options)))->write(
            $path,
            $schema,
            array_map(static fn(int $id): array => ['id' => $id], range(1, 9)),
        );

        static::assertSame($expected, array_map(
            /** @param array<string, list<mixed>>|SkippedMatches $chunk */
            static fn(array|SkippedMatches $chunk): array => (
                $chunk instanceof SkippedMatches ? ['skipped' => $chunk->count] : $chunk['id']
            ),
            iterator_to_array(
                ParquetEngineMother::create($engineClass)
                    ->openForRead(NativeLocalSourceStream::open(path_real($path)))
                    ->readColumns(
                        ['id'],
                        100,
                        ReadPlanMother::rowFiltered(9, parquet_column('id')->greaterThan(2), $schema, offset: $offset),
                    ),
                false,
            ),
        ));
    }

    /**
     * @param int<1, max> $batchSize
     * @param list<int> $sizes
     * @param list<list<int>> $ids
     */
    #[DataProvider('chunkings')]
    public function test_chunks_follow_batch_size_limit_and_offset(
        string $engineClass,
        int $batchSize,
        ?int $limit,
        ?int $offset,
        array $sizes,
        array $ids,
    ): void {
        $engine = ParquetEngineMother::create($engineClass);
        $path = TestParquetFile::path($this);

        (new Writer())->write(
            $path,
            Schema::with(
                FlatColumn::int32('id', Repetition::REQUIRED),
                NestedColumn::list('tags', ListElement::string()),
            ),
            array_map(static fn(int $i): array => ['id' => $i, 'tags' => ['t' . $i]], [1, 2, 3, 4, 5]),
        );

        /** @var list<array<string, list<mixed>>> $chunks */
        $chunks = iterator_to_array(
            $engine->openForRead(NativeLocalSourceStream::open(path_real($path)))->readColumns(
                ['id', 'tags'],
                $batchSize,
                ReadPlanMother::window(5, $offset ?? 0, $limit),
            ),
            false,
        );

        static::assertSame($sizes, array_map(static fn(array $chunk): int => count($chunk['id']), $chunks));
        static::assertSame($ids, array_map(static fn(array $chunk): array => $chunk['id'], $chunks));
        static::assertSame(
            array_map(static fn(array $chunk): array => array_map(static fn(int $i): array => [
                't' . $i,
            ], $chunk), $ids),
            array_map(static fn(array $chunk): array => $chunk['tags'], $chunks),
        );
    }
}
