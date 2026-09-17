<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Explain;

use Flow\ETL\Plan\Explain\Branches;
use Flow\ETL\Plan\Explain\Entry;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class BranchesTest extends FlowTestCase
{
    public function test_a_leaf_has_no_branches(): void
    {
        static::assertSame([], (new Branches())->of(new Entry(NodeMother::read(), 1, false, []), '│  '));
    }

    public function test_every_child_but_the_last_keeps_the_rail_open(): void
    {
        $first = new Entry(NodeMother::read(), 2, false, []);
        $second = new Entry(NodeMother::read(), 3, false, []);
        $third = new Entry(NodeMother::read(), 4, false, []);

        static::assertSame(
            [
                [$first,  '│  ├─ ', '│  │  '],
                [$second, '│  ├─ ', '│  │  '],
                [$third,  '│  └─ ', '│     '],
            ],
            (new Branches())->of(new Entry(NodeMother::read(), 1, false, [$first, $second, $third]), '│  '),
        );
    }
}
