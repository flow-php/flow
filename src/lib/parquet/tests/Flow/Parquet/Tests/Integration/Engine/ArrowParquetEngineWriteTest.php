<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use function Flow\ETL\DSL\generate_random_string;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\{Options, Reader};
use Flow\Parquet\ParquetFile\{Compressions, Schema};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn, Repetition};
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

#[Group('native-extension')]
final class ArrowParquetEngineWriteTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('arrow')) {
            self::markTestSkipped('Arrow extension is not loaded');
        }

        if (!\file_exists(__DIR__ . '/var')) {
            \mkdir(__DIR__ . '/var');
        }
    }

    public function test_write_flat_columns_readable_by_php_engine() : void
    {
        $path = __DIR__ . '/var/test-arrow-write-flat-' . generate_random_string() . '.parquet';

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
        $stream = \Flow\Filesystem\Stream\NativeLocalDestinationStream::openBlank(\Flow\Filesystem\DSL\path($path));
        $engine->writeRows($stream, $schema, Compressions::SNAPPY, new Options(), $inputData);

        $result = \iterator_to_array((new Reader())->read($path)->values());

        self::assertCount(3, $result);
        self::assertSame(1, $result[0]['id']);
        self::assertSame('Alice', $result[0]['name']);
        self::assertTrue($result[0]['active']);
        self::assertEqualsWithDelta(99.5, $result[0]['score'], 0.001);

        \unlink($path);
    }

    public function test_write_nested_types() : void
    {
        $path = __DIR__ . '/var/test-arrow-write-nested-' . generate_random_string() . '.parquet';

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
        $stream = \Flow\Filesystem\Stream\NativeLocalDestinationStream::openBlank(\Flow\Filesystem\DSL\path($path));
        $engine->writeRows($stream, $schema, Compressions::SNAPPY, new Options(), $inputData);

        $result = \iterator_to_array((new Reader())->read($path)->values());

        self::assertCount(2, $result);
        self::assertSame(1, $result[0]['id']);

        \unlink($path);
    }

    public function test_write_with_gzip_compression() : void
    {
        $path = __DIR__ . '/var/test-arrow-write-gzip-' . generate_random_string() . '.parquet';

        $schema = Schema::with(
            FlatColumn::int32('id', Repetition::REQUIRED),
            FlatColumn::string('data'),
        );

        $inputData = \array_map(
            static fn (int $i) : array => ['id' => $i, 'data' => 'value_' . $i],
            \range(1, 20),
        );

        $engine = new ArrowParquetEngine();
        $stream = \Flow\Filesystem\Stream\NativeLocalDestinationStream::openBlank(\Flow\Filesystem\DSL\path($path));
        $engine->writeRows($stream, $schema, Compressions::GZIP, new Options(), $inputData);

        $result = \iterator_to_array((new Reader())->read($path)->values());

        self::assertCount(20, $result);

        \unlink($path);
    }

    public function test_write_with_snappy_compression() : void
    {
        $path = __DIR__ . '/var/test-arrow-write-snappy-' . generate_random_string() . '.parquet';

        $schema = Schema::with(
            FlatColumn::int32('id', Repetition::REQUIRED),
            FlatColumn::string('data'),
        );

        $inputData = \array_map(
            static fn (int $i) : array => ['id' => $i, 'data' => \str_repeat('a', 100)],
            \range(1, 50),
        );

        $engine = new ArrowParquetEngine();
        $stream = \Flow\Filesystem\Stream\NativeLocalDestinationStream::openBlank(\Flow\Filesystem\DSL\path($path));
        $engine->writeRows($stream, $schema, Compressions::SNAPPY, new Options(), $inputData);

        $result = \iterator_to_array((new Reader())->read($path)->values());

        self::assertCount(50, $result);
        self::assertSame(1, $result[0]['id']);

        \unlink($path);
    }
}
