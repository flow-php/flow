<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Loader;

use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\FakeStaticOrdersExtractor;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\LimitTransformer;

use function array_column;
use function file_get_contents;
use function Flow\ETL\DSL\add_row_index;
use function Flow\ETL\DSL\batch_size;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\drop;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\limit;
use function Flow\ETL\DSL\mask_columns;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\to_stream;
use function Flow\ETL\DSL\to_transformation;

final class TransformerLoaderTest extends FlowIntegrationTestCase
{
    public function test_transformer_loader_with_add_row_index_transformation(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([
                ['name' => 'Alice', 'age' => 30],
                ['name' => 'Bob', 'age' => 25],
                ['name' => 'Charlie', 'age' => 35],
            ]))
            ->collect()
            ->write(to_transformation(add_row_index('row_num', StartFrom::ONE), to_memory($memory)))
            ->run();

        static::assertSame(
            [
                ['name' => 'Alice', 'age' => 30, 'row_num' => 1],
                ['name' => 'Bob', 'age' => 25, 'row_num' => 2],
                ['name' => 'Charlie', 'age' => 35, 'row_num' => 3],
            ],
            $memory->dump(),
        );
    }

    public function test_transformer_loader_with_batch_size_transformation(): void
    {
        $loader = new SpyLoader();

        df()
            ->read(new FakeStaticOrdersExtractor(1000))
            ->collect()
            ->write(to_transformation(batch_size(500), $loader))
            ->run();

        static::assertSame(2, $loader->loadsCount);
    }

    public function test_transformer_loader_with_add_row_index_transformation_across_batches(): void
    {
        $source = [];

        for ($id = 1; $id <= 6; $id++) {
            $source[] = ['id' => $id];
        }

        $memory = new ArrayMemory();

        df()
            ->read(from_array($source))
            ->write(to_transformation(add_row_index('n', StartFrom::ONE), to_memory($memory)))
            ->run();

        static::assertSame([1, 2, 3, 4, 5, 6], array_column($memory->dump(), 'n'));
    }

    public function test_transformer_loader_with_batch_size_transformation_across_batches(): void
    {
        $source = [];

        for ($id = 1; $id <= 6; $id++) {
            $source[] = ['id' => $id];
        }

        $loader = new SpyLoader();

        df()
            ->read(from_array($source))
            ->write(to_transformation(batch_size(4), $loader))
            ->run();

        static::assertSame(6, $loader->loadsCount);
    }

    public function test_transformer_loader_with_drop_transformation(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com', 'password' => 'secret123'],
                ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com', 'password' => 'secret456'],
            ]))
            ->write(to_transformation(drop('password', 'email'), to_memory($memory)))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
            ],
            $memory->dump(),
        );
    }

    public function test_transformer_loader_with_limit_transformer_does_not_stop_sibling_loaders(): void
    {
        $limited = new ArrayMemory();
        $sibling = new ArrayMemory();

        $source = [];

        for ($id = 1; $id <= 20; $id++) {
            $source[] = ['id' => $id];
        }

        df()
            ->read(from_array($source))
            ->load(to_transformation(new LimitTransformer(10), to_memory($limited)))
            ->load(to_memory($sibling))
            ->run();

        static::assertCount(10, $limited->dump());
        static::assertCount(20, $sibling->dump());
    }

    public function test_transformer_loader_with_limit_transformation(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
                ['id' => 3, 'name' => 'Charlie'],
                ['id' => 4, 'name' => 'Diana'],
                ['id' => 5, 'name' => 'Eve'],
            ]))
            ->collect()
            ->write(to_transformation(limit(3), to_memory($memory)))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
                ['id' => 3, 'name' => 'Charlie'],
            ],
            $memory->dump(),
        );
    }

    public function test_transformer_loader_with_limit_transformation_across_batches(): void
    {
        $source = [];

        for ($id = 1; $id <= 6; $id++) {
            $source[] = ['id' => $id];
        }

        $memory = new ArrayMemory();

        df()
            ->read(from_array($source))
            ->write(to_transformation(limit(3), to_memory($memory)))
            ->run();

        static::assertSame([['id' => 1], ['id' => 2], ['id' => 3]], $memory->dump());
    }

    public function test_transformer_loader_with_mask_columns_transformation(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'ssn' => '123-45-6789', 'email' => 'alice@example.com'],
                ['id' => 2, 'name' => 'Bob', 'ssn' => '987-65-4321', 'email' => 'bob@example.com'],
            ]))
            ->write(to_transformation(mask_columns(['ssn', 'email'], '***'), to_memory($memory)))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'name' => 'Alice', 'ssn' => '***', 'email' => '***'],
                ['id' => 2, 'name' => 'Bob', 'ssn' => '***', 'email' => '***'],
            ],
            $memory->dump(),
        );
    }

    public function test_transformer_loader_with_select_transformation(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com', 'age' => 30],
                ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com', 'age' => 25],
            ]))
            ->write(to_transformation(select('name', 'email'), to_memory($memory)))
            ->run();

        static::assertSame(
            [
                ['name' => 'Alice', 'email' => 'alice@example.com'],
                ['name' => 'Bob', 'email' => 'bob@example.com'],
            ],
            $memory->dump(),
        );
    }

    public function test_transformer_loader_with_stream_loader_across_batches(): void
    {
        df()
            ->read(from_sequence_number('id', 1, 12))
            ->batchSize(4)
            ->write(to_transformation(
                select('id'),
                to_stream(
                    $path = $this->cacheDir->suffix('transformation_stream.txt')->path(),
                    output: Output::rows_count,
                ),
            ))
            ->run();

        $content = file_get_contents($path);

        if ($content === false) {
            static::fail('Failed to read file content');
        }

        static::assertSame("Rows: 4\nRows: 4\nRows: 4\n", $content);
    }
}
