<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Explain;

use Flow\ETL\Plan\Explain\Branches;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\EntryMother;

final class BranchesTest extends FlowTestCase
{
    public function test_a_leaf_has_no_branches(): void
    {
        static::assertSame([], (new Branches())->of(EntryMother::named('Read', 1), '│  '));
    }

    public function test_every_child_but_the_last_keeps_the_rail_open(): void
    {
        $first = EntryMother::named('Read', 2);
        $second = EntryMother::named('Read', 3);
        $third = EntryMother::named('Read', 4);

        static::assertSame(
            [
                [$first,  '│  ├─ ', '│  │  '],
                [$second, '│  ├─ ', '│  │  '],
                [$third,  '│  └─ ', '│     '],
            ],
            (new Branches())->of(EntryMother::named('Read', 1, [$first, $second, $third]), '│  '),
        );
    }
}
