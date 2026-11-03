<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\aggregate;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\group_by;
use function Flow\ETL\DSL\pivot;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;

final class PivotTest extends FlowTestCase
{
    public function test_pivot_pivots_the_grouped_frame(): void
    {
        static::assertSame(
            [['d' => 'x', 'PL' => 1.0, 'US' => 2.0], ['d' => 'y', 'PL' => 3.0, 'US' => null]],
            aggregate([sum(ref('a'))])(pivot(ref('c'), pivot_values('PL', 'US'))(group_by('d')(df()->read(from_array([
                ['d' => 'x', 'c' => 'PL', 'a' => 1],
                ['d' => 'x', 'c' => 'US', 'a' => 2],
                ['d' => 'y', 'c' => 'PL', 'a' => 3],
            ])))))
                ->fetch()
                ->toArray(),
        );
    }
}
