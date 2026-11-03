<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\drop;
use function Flow\ETL\DSL\from_array;

final class DropTest extends FlowTestCase
{
    public function test_invoking_drop_gives_the_rows_of_with(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']]));
        $expected = [['name' => 'a'], ['name' => 'b']];

        static::assertSame($expected, drop('id')($dataFrame)->fetch()->toArray());
        static::assertSame($expected, $dataFrame->with(drop('id'))->fetch()->toArray());
    }
}
