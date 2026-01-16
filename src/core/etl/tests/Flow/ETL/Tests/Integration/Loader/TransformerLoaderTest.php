<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Loader;

use function Flow\ETL\DSL\{add_row_index, batch_size, df, drop, from_array, limit, mask_columns, select, to_memory, to_transformation};
use Flow\ETL\Loader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\FakeStaticOrdersExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;

final class TransformerLoaderTest extends FlowTestCase
{
    public function test_transformer_loader_with_add_row_index_transformation() : void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([
                ['name' => 'Alice', 'age' => 30],
                ['name' => 'Bob', 'age' => 25],
                ['name' => 'Charlie', 'age' => 35],
            ]))
            ->collect()
            ->write(
                to_transformation(
                    add_row_index('row_num', StartFrom::ONE),
                    to_memory($memory)
                )
            )
            ->run();

        self::assertSame(
            [
                ['name' => 'Alice', 'age' => 30, 'row_num' => 1],
                ['name' => 'Bob', 'age' => 25, 'row_num' => 2],
                ['name' => 'Charlie', 'age' => 35, 'row_num' => 3],
            ],
            $memory->dump()
        );
    }

    public function test_transformer_loader_with_batch_size_transformation() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))
            ->method('load');

        df()
             ->read(
                 new FakeStaticOrdersExtractor(1000)
             )
             ->collect()
             ->write(
                 to_transformation(
                     batch_size(500),
                     $loader
                 )
             )
             ->run();
    }

    public function test_transformer_loader_with_drop_transformation() : void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com', 'password' => 'secret123'],
                ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com', 'password' => 'secret456'],
            ]))
            ->write(
                to_transformation(
                    drop('password', 'email'),
                    to_memory($memory)
                )
            )
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
            ],
            $memory->dump()
        );
    }

    public function test_transformer_loader_with_limit_transformation() : void
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
            ->write(
                to_transformation(
                    limit(3),
                    to_memory($memory)
                )
            )
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
                ['id' => 3, 'name' => 'Charlie'],
            ],
            $memory->dump()
        );
    }

    public function test_transformer_loader_with_mask_columns_transformation() : void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'ssn' => '123-45-6789', 'email' => 'alice@example.com'],
                ['id' => 2, 'name' => 'Bob', 'ssn' => '987-65-4321', 'email' => 'bob@example.com'],
            ]))
            ->write(
                to_transformation(
                    mask_columns(['ssn', 'email'], '***'),
                    to_memory($memory)
                )
            )
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'name' => 'Alice', 'ssn' => '***', 'email' => '***'],
                ['id' => 2, 'name' => 'Bob', 'ssn' => '***', 'email' => '***'],
            ],
            $memory->dump()
        );
    }

    public function test_transformer_loader_with_select_transformation() : void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com', 'age' => 30],
                ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com', 'age' => 25],
            ]))
            ->write(
                to_transformation(
                    select('name', 'email'),
                    to_memory($memory)
                )
            )
            ->run();

        self::assertSame(
            [
                ['name' => 'Alice', 'email' => 'alice@example.com'],
                ['name' => 'Bob', 'email' => 'bob@example.com'],
            ],
            $memory->dump()
        );
    }
}
