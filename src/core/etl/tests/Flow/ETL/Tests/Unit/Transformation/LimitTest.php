<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformation;

use function Flow\ETL\DSL\{data_frame, from_array};
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation\Limit;

final class LimitTest extends FlowTestCase
{
    public function test_limit_transformation() : void
    {
        $rows = data_frame()
            ->read(from_array([
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Doe'],
                ['id' => 3, 'name' => 'Smith'],
            ]))
            ->with(new Limit(2))
            ->fetch()
            ->toArray();

        self::assertCount(2, $rows);
    }
}
