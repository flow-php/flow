<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\select;

final class SelectTest extends FlowTestCase
{
    public function test_invoking_select_gives_the_rows_of_with(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']]));
        $expected = [['id' => 1], ['id' => 2]];

        static::assertSame($expected, select('id')($dataFrame)->fetch()->toArray());
        static::assertSame($expected, $dataFrame->with(select('id'))->fetch()->toArray());
    }
}
