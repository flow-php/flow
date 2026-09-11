<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Flow\Filesystem\Stream\NativeLocalDestinationStream;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\TestParquetFile;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

use function array_map;
use function extension_loaded;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function range;
use function str_repeat;

#[Group('native-extension')]
final class ArrowParquetEngineWriteTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('arrow')) {
            self::markTestSkipped('Arrow extension is not loaded');
        }
    }

    protected function tearDown(): void
    {
        TestParquetFile::remove($this);
    }

    public function test_write_flat_columns_readable_by_php_engine(): void
    {
        $path = TestParquetFile::path($this);

        $schema = Schema::with(
            FlatColumn::int32('id', Repetition::REQUIRED),
            FlatColumn::string('name'),
            FlatColumn::boolean('active'),
            FlatColumn::double('score'),
        );

        $inputData = [
            ['id' => 1, 'name' => 'Alice', 'active' => true, 'score' => 99.5],
            ['id' => 2, 'name' => 'Bob', 'active' => false, 'score' => 87.3],
            ['id' => 3, 'name' => 'Charlie', 'active' => true, 'score' => 92.1],
        ];

        $engine = new ArrowParquetEngine();
        $stream = NativeLocalDestinationStream::openBlank(path($path));
        $engine->writeRows($stream, $schema, Compressions::SNAPPY, new Options(), $inputData);

        $result = iterator_to_array(
            (new Reader())
                ->read($path)
                ->values(),
        );

        static::assertCount(3, $result);
        static::assertSame(1, $result[0]['id']);
        static::assertSame('Alice', $result[0]['name']);
        static::assertTrue($result[0]['active']);
        static::assertEqualsWithDelta(99.5, $result[0]['score'], 0.001);
    }

    public function test_write_nested_types(): void
    {
        $path = TestParquetFile::path($this);

        $schema = Schema::with(
            FlatColumn::int32('id', Repetition::REQUIRED),
            NestedColumn::list('tags', ListElement::string()),
            NestedColumn::struct('address', [
                FlatColumn::string('city'),
            ]),
        );

        $inputData = [
            ['id' => 1, 'tags' => ['php', 'rust'], 'address' => ['city' => 'Berlin']],
            ['id' => 2, 'tags' => ['python'], 'address' => ['city' => 'Warsaw']],
        ];

        $engine = new ArrowParquetEngine();
        $stream = NativeLocalDestinationStream::openBlank(path($path));
        $engine->writeRows($stream, $schema, Compressions::SNAPPY, new Options(), $inputData);

        $result = iterator_to_array(
            (new Reader())
                ->read($path)
                ->values(),
        );

        static::assertCount(2, $result);
        static::assertSame(1, $result[0]['id']);
    }

    public function test_write_with_gzip_compression(): void
    {
        $path = TestParquetFile::path($this);

        $schema = Schema::with(FlatColumn::int32('id', Repetition::REQUIRED), FlatColumn::string('data'));

        $inputData = array_map(static fn(int $i): array => ['id' => $i, 'data' => 'value_' . $i], range(1, 20));

        $engine = new ArrowParquetEngine();
        $stream = NativeLocalDestinationStream::openBlank(path($path));
        $engine->writeRows($stream, $schema, Compressions::GZIP, new Options(), $inputData);

        $result = iterator_to_array(
            (new Reader())
                ->read($path)
                ->values(),
        );

        static::assertCount(20, $result);
    }

    public function test_write_with_snappy_compression(): void
    {
        $path = TestParquetFile::path($this);

        $schema = Schema::with(FlatColumn::int32('id', Repetition::REQUIRED), FlatColumn::string('data'));

        $inputData = array_map(static fn(int $i): array => ['id' => $i, 'data' => str_repeat('a', 100)], range(1, 50));

        $engine = new ArrowParquetEngine();
        $stream = NativeLocalDestinationStream::openBlank(path($path));
        $engine->writeRows($stream, $schema, Compressions::SNAPPY, new Options(), $inputData);

        $result = iterator_to_array(
            (new Reader())
                ->read($path)
                ->values(),
        );

        static::assertCount(50, $result);
        static::assertSame(1, $result[0]['id']);
    }
}
