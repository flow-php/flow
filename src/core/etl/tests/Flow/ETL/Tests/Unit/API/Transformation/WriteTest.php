<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\write;

final class WriteTest extends FlowTestCase
{
    public function test_write_through_invoke(): void
    {
        $memory = new ArrayMemory();

        write(to_memory($memory))(df()->read(from_array([['a' => 1], ['a' => 2]])))->run();

        static::assertSame([['a' => 1], ['a' => 2]], $memory->dump());
    }

    public function test_write_through_with(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([['a' => 1], ['a' => 2]]))
            ->with(write(to_memory($memory)))
            ->run();

        static::assertSame([['a' => 1], ['a' => 2]], $memory->dump());
    }
}
