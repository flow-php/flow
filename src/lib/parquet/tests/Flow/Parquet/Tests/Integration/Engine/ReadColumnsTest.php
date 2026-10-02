<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Tests\Integration\IO\ParquetIntegrationTestCase;
use Flow\Parquet\Tests\Mother\ParquetEngineMother;
use Flow\Parquet\Writer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function count;
use function Flow\Filesystem\DSL\path_real;
use function iterator_to_array;

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

        $chunks = iterator_to_array(
            $engine->openForRead(NativeLocalSourceStream::open(path_real($path)))->readColumns(
                ['id', 'tags'],
                $batchSize,
                $limit,
                $offset,
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
