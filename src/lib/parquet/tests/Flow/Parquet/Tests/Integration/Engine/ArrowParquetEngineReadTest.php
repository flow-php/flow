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
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

use function array_map;
use function extension_loaded;
use function Flow\Filesystem\DSL\path_real;
use function iterator_to_array;
use function range;

#[Group('native-extension')]
final class ArrowParquetEngineReadTest extends TestCase
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

    public function test_read_flat_columns(): void
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
    }

    public function test_read_nested_lists(): void
    {
        $path = TestParquetFile::path($this);

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
    }

    public function test_read_nested_maps(): void
    {
        $path = TestParquetFile::path($this);

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
    }

    public function test_read_nested_structs(): void
    {
        $path = TestParquetFile::path($this);

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
    }

    public function test_read_nested_uuid_as_canonical_strings(): void
    {
        $path = TestParquetFile::path($this);
        $uuid = 'f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a';

        $schema = Schema::with(
            FlatColumn::uuid('top_uuid'),
            NestedColumn::struct('body', [FlatColumn::uuid('id'), FlatColumn::json('data')]),
            NestedColumn::list('uuid_list', ListElement::uuid()),
        );

        (new Writer())->write($path, $schema, [[
            'top_uuid' => $uuid,
            'body' => ['id' => $uuid, 'data' => '{"a":1}'],
            'uuid_list' => [$uuid, $uuid],
        ]]);

        $engine = new ArrowParquetEngine();
        $parquetFile = (new Reader())->read($path);
        $result = iterator_to_array($engine->readValues(
            NativeLocalSourceStream::open(path_real($path)),
            $parquetFile->schema(),
        ));

        static::assertSame($uuid, $result[0]['top_uuid']);
        static::assertSame(['id' => $uuid, 'data' => '{"a":1}'], $result[0]['body']);
        static::assertSame([$uuid, $uuid], $result[0]['uuid_list']);
    }

    public function test_read_with_column_projection(): void
    {
        $path = TestParquetFile::path($this);

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
    }

    public function test_read_with_limit(): void
    {
        $path = TestParquetFile::path($this);

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
    }

    public function test_read_with_limit_and_offset(): void
    {
        $path = TestParquetFile::path($this);

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
    }

    public function test_read_with_offset(): void
    {
        $path = TestParquetFile::path($this);

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
    }
}
