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
        if (!extension_loaded('flow_php') && !extension_loaded('arrow')) {
            self::markTestSkipped('Neither flow_php nor arrow is loaded');
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
        $chunks = iterator_to_array(
            $engine->openForRead(NativeLocalSourceStream::open(path_real($path)))->readColumns(
                ['id', 'name', 'active', 'score'],
                batchSize: 2,
                limit: null,
                offset: null,
            ),
            false,
        );

        static::assertSame(
            [
                ['id' => [1, 2], 'name' => ['Alice', 'Bob'], 'active' => [true, false], 'score' => [99.5, 87.3]],
                ['id' => [3], 'name' => ['Charlie'], 'active' => [true], 'score' => [92.1]],
            ],
            $chunks,
        );
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
        $chunks = iterator_to_array(
            $engine->openForRead(NativeLocalSourceStream::open(path_real($path)))->readColumns(
                ['id', 'tags'],
                batchSize: 2,
                limit: null,
                offset: null,
            ),
            false,
        );

        static::assertSame([['id' => [1, 2], 'tags' => [['php', 'rust'], ['python']]]], $chunks);
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
        $chunks = iterator_to_array(
            $engine->openForRead(NativeLocalSourceStream::open(path_real($path)))->readColumns(
                ['id', 'metadata'],
                batchSize: 2,
                limit: null,
                offset: null,
            ),
            false,
        );

        static::assertSame(
            [['id' => [1, 2], 'metadata' => [['score' => 100, 'level' => 5], ['score' => 200]]]],
            $chunks,
        );
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
        $chunks = iterator_to_array(
            $engine->openForRead(NativeLocalSourceStream::open(path_real($path)))->readColumns(
                ['id', 'address'],
                batchSize: 2,
                limit: null,
                offset: null,
            ),
            false,
        );

        static::assertSame(
            [['id' => [1, 2], 'address' => [['city' => 'Berlin', 'zip' => 10115], ['city' => 'Warsaw', 'zip' => 1]]]],
            $chunks,
        );
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
        $chunks = iterator_to_array(
            $engine->openForRead(NativeLocalSourceStream::open(path_real($path)))->readColumns(
                ['top_uuid', 'body', 'uuid_list'],
                batchSize: 2,
                limit: null,
                offset: null,
            ),
            false,
        );

        static::assertSame(
            [[
                'top_uuid' => [$uuid],
                'body' => [['id' => $uuid, 'data' => '{"a":1}']],
                'uuid_list' => [[$uuid, $uuid]],
            ]],
            $chunks,
        );
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
        $chunks = iterator_to_array(
            $engine->openForRead(NativeLocalSourceStream::open(path_real($path)))->readColumns(
                ['id', 'name'],
                batchSize: 2,
                limit: null,
                offset: null,
            ),
            false,
        );

        static::assertSame([['id' => [1, 2], 'name' => ['Alice', 'Bob']]], $chunks);
    }

    public function test_read_with_limit(): void
    {
        $path = TestParquetFile::path($this);

        $schema = Schema::with(FlatColumn::int32('id', Repetition::REQUIRED));

        $inputData = array_map(static fn(int $i): array => ['id' => $i], range(1, 100));

        (new Writer())->write($path, $schema, $inputData);

        $engine = new ArrowParquetEngine();
        $chunks = iterator_to_array(
            $engine->openForRead(NativeLocalSourceStream::open(path_real($path)))->readColumns(
                ['id'],
                batchSize: 2,
                limit: 5,
                offset: null,
            ),
            false,
        );

        static::assertSame([['id' => [1, 2]], ['id' => [3, 4]], ['id' => [5]]], $chunks);
    }

    public function test_read_with_limit_and_offset(): void
    {
        $path = TestParquetFile::path($this);

        $schema = Schema::with(FlatColumn::int32('id', Repetition::REQUIRED));

        $inputData = array_map(static fn(int $i): array => ['id' => $i], range(1, 100));

        (new Writer())->write($path, $schema, $inputData);

        $engine = new ArrowParquetEngine();
        $chunks = iterator_to_array(
            $engine->openForRead(NativeLocalSourceStream::open(path_real($path)))->readColumns(
                ['id'],
                batchSize: 2,
                limit: 5,
                offset: 10,
            ),
            false,
        );

        static::assertSame([['id' => [11, 12]], ['id' => [13, 14]], ['id' => [15]]], $chunks);
    }

    public function test_read_with_offset(): void
    {
        $path = TestParquetFile::path($this);

        $schema = Schema::with(FlatColumn::int32('id', Repetition::REQUIRED));

        $inputData = array_map(static fn(int $i): array => ['id' => $i], range(1, 100));

        (new Writer())->write($path, $schema, $inputData);

        $engine = new ArrowParquetEngine();
        $chunks = iterator_to_array(
            $engine->openForRead(NativeLocalSourceStream::open(path_real($path)))->readColumns(
                ['id'],
                batchSize: 2,
                limit: null,
                offset: 96,
            ),
            false,
        );

        static::assertSame([['id' => [97, 98]], ['id' => [99, 100]]], $chunks);
    }
}
