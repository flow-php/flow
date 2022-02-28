<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Factory;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Factory\ArrayRowsFactory;
use PHPUnit\Framework\TestCase;

final class ArrayRowsFactoryTest extends TestCase
{
    public function test_create_rows_from_array() : void
    {
        $factory = new ArrayRowsFactory();

        $rows = $factory->create($data = [['id' => 1], ['id' => 2], ['id' => 3]]);

        $this->assertSame($data, $rows->toArray());
    }

    public function test_creating_rows_from_flat_array() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayRowsFactory expects data to be an array of arrays');

        (new ArrayRowsFactory())->create([1, 2, 3]);
    }
}
