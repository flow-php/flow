<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\group_by;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;

final class GroupByTest extends FlowTestCase
{
    public function test_group_by_groups_the_frame(): void
    {
        static::assertSame(
            [['country' => 'PL', 'a_sum' => 3.0], ['country' => 'US', 'a_sum' => 3.0]],
            group_by('country')(df()->read(from_array([
                ['country' => 'PL', 'a' => 1],
                ['country' => 'PL', 'a' => 2],
                ['country' => 'US', 'a' => 3],
            ])))
                ->aggregate(sum(ref('a')))
                ->fetch()
                ->toArray(),
        );
    }
}
