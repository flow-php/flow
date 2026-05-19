<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

use function array_map;
use function extension_loaded;
use function file_exists;
use function Flow\ETL\DSL\generate_random_string;
use function Flow\Filesystem\DSL\path_real;
use function iterator_to_array;
use function mkdir;
use function range;
use function unlink;

#[Group('native-extension')]
final class ArrowParquetEngineReadTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('arrow')) {
            self::markTestSkipped('Arrow extension is not loaded');
        }

        if (!file_exists(__DIR__ . '/var')) {
            mkdir(__DIR__ . '/var');
        }
    }

    public function test_read_flat_columns(): void
    {
        $path = __DIR__ . '/var/test-arrow-read-flat-' . generate_random_string() . '.parquet';

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

        (new Writer())->write($path, $schema, $inputData);

        $engine = new ArrowParquetEngine();
        $parquetFile = (new Reader())->read($path);
        $result = iterator_to_array($engine->readValues(
            NativeLocalSourceStream::open(path_real($path)),
            $parquetFile->schema(),
        ));

        static::assertCount(3, $result);
        static::assertSame(1, $result[0]['id']);
        static::assertSame('Alice', $result[0]['name']);
        static::assertTrue($result[0]['active']);
        static::assertEqualsWithDelta(99.5, $result[0]['score'], 0.001);

        unlink($path);
    }

    public function test_read_nested_lists(): void
    {
        $path = __DIR__ . '/var/test-arrow-read-lists-' . generate_random_string() . '.parquet';

        $schema = Schema::with(
            FlatColumn::int32('id', Repetition::REQUIRED),
            NestedColumn::list('tags', ListElement::string()),
        );

        $inputData = [
            ['id' => 1, 'tags' => ['php', 'rust']],
            ['id' => 2, 'tags' => ['python']],
        ];

        (new Writer())->write($path, $schema, $inputData);

        $engine = new ArrowParquetEngine();
        $parquetFile = (new Reader())->read($path);
        $result = iterator_to_array($engine->readValues(
            NativeLocalSourceStream::open(path_real($path)),
            $parquetFile->schema(),
        ));

        static::assertCount(2, $result);
        static::assertSame(['php', 'rust'], $result[0]['tags']);
        static::assertSame(['python'], $result[1]['tags']);

        unlink($path);
    }

    public function test_read_nested_maps(): void
    {
        $path = __DIR__ . '/var/test-arrow-read-maps-' . generate_random_string() . '.parquet';

        $schema = Schema::with(
            FlatColumn::int32('id', Repetition::REQUIRED),
            NestedColumn::map('metadata', MapKey::string(), MapValue::int32()),
        );

        $inputData = [
            ['id' => 1, 'metadata' => ['score' => 100, 'level' => 5]],
            ['id' => 2, 'metadata' => ['score' => 200]],
        ];

        (new Writer())->write($path, $schema, $inputData);

        $engine = new ArrowParquetEngine();
        $parquetFile = (new Reader())->read($path);
        $result = iterator_to_array($engine->readValues(
            NativeLocalSourceStream::open(path_real($path)),
            $parquetFile->schema(),
        ));

        static::assertCount(2, $result);
        /** @var array<string, mixed> $metadata */
        $metadata = $result[0]['metadata'];
        static::assertIsArray($metadata);
        static::assertSame(100, $metadata['score']);
        static::assertSame(5, $metadata['level']);

        unlink($path);
    }

    public function test_read_nested_structs(): void
    {
        $path = __DIR__ . '/var/test-arrow-read-structs-' . generate_random_string() . '.parquet';

        $schema = Schema::with(
            FlatColumn::int32('id', Repetition::REQUIRED),
            NestedColumn::struct('address', [
                FlatColumn::string('city'),
                FlatColumn::int32('zip'),
            ]),
        );

        $inputData = [
            ['id' => 1, 'address' => ['city' => 'Berlin', 'zip' => 10115]],
            ['id' => 2, 'address' => ['city' => 'Warsaw', 'zip' => 00001]],
        ];

        (new Writer())->write($path, $schema, $inputData);

        $engine = new ArrowParquetEngine();
        $parquetFile = (new Reader())->read($path);
        $result = iterator_to_array($engine->readValues(
            NativeLocalSourceStream::open(path_real($path)),
            $parquetFile->schema(),
        ));

        static::assertCount(2, $result);
        /** @var array<string, mixed> $address */
        $address = $result[0]['address'];
        static::assertIsArray($address);
        static::assertSame('Berlin', $address['city']);
        static::assertSame(10115, $address['zip']);

        unlink($path);
    }

    public function test_read_with_column_projection(): void
    {
        $path = __DIR__ . '/var/test-arrow-read-proj-' . generate_random_string() . '.parquet';

        $schema = Schema::with(
            FlatColumn::int32('id', Repetition::REQUIRED),
            FlatColumn::string('name'),
            FlatColumn::string('email'),
        );

        $inputData = [
            ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'],
            ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com'],
        ];

        (new Writer())->write($path, $schema, $inputData);

        $engine = new ArrowParquetEngine();
        $parquetFile = (new Reader())->read($path);
        $result = iterator_to_array($engine->readValues(
            NativeLocalSourceStream::open(path_real($path)),
            $parquetFile->schema(),
            ['id', 'name'],
        ));

        static::assertCount(2, $result);
        static::assertArrayHasKey('id', $result[0]);
        static::assertArrayHasKey('name', $result[0]);
        static::assertArrayNotHasKey('email', $result[0]);

        unlink($path);
    }

    public function test_read_with_limit(): void
    {
        $path = __DIR__ . '/var/test-arrow-read-limit-' . generate_random_string() . '.parquet';

        $schema = Schema::with(FlatColumn::int32('id', Repetition::REQUIRED));

        $inputData = array_map(static fn(int $i): array => ['id' => $i], range(1, 100));

        (new Writer())->write($path, $schema, $inputData);

        $engine = new ArrowParquetEngine();
        $parquetFile = (new Reader())->read($path);
        $result = iterator_to_array($engine->readValues(
            NativeLocalSourceStream::open(path_real($path)),
            $parquetFile->schema(),
            [],
            10,
        ));

        static::assertCount(10, $result);
        static::assertSame(1, $result[0]['id']);
        static::assertSame(10, $result[9]['id']);

        unlink($path);
    }

    public function test_read_with_limit_and_offset(): void
    {
        $path = __DIR__ . '/var/test-arrow-read-limit-offset-' . generate_random_string() . '.parquet';

        $schema = Schema::with(FlatColumn::int32('id', Repetition::REQUIRED));

        $inputData = array_map(static fn(int $i): array => ['id' => $i], range(1, 100));

        (new Writer())->write($path, $schema, $inputData);

        $engine = new ArrowParquetEngine();
        $parquetFile = (new Reader())->read($path);
        $result = iterator_to_array($engine->readValues(
            NativeLocalSourceStream::open(path_real($path)),
            $parquetFile->schema(),
            [],
            5,
            10,
        ));

        static::assertCount(5, $result);
        static::assertSame(11, $result[0]['id']);
        static::assertSame(15, $result[4]['id']);

        unlink($path);
    }

    public function test_read_with_offset(): void
    {
        $path = __DIR__ . '/var/test-arrow-read-offset-' . generate_random_string() . '.parquet';

        $schema = Schema::with(FlatColumn::int32('id', Repetition::REQUIRED));

        $inputData = array_map(static fn(int $i): array => ['id' => $i], range(1, 100));

        (new Writer())->write($path, $schema, $inputData);

        $engine = new ArrowParquetEngine();
        $parquetFile = (new Reader())->read($path);
        $result = iterator_to_array($engine->readValues(
            NativeLocalSourceStream::open(path_real($path)),
            $parquetFile->schema(),
            [],
            null,
            50,
        ));

        static::assertCount(50, $result);
        static::assertSame(51, $result[0]['id']);

        unlink($path);
    }
}
