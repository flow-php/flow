<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Plan\ReplaceLeaf;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class ReplaceLeafTest extends FlowTestCase
{
    public function test_the_target_node_is_replaced(): void
    {
        $target = NodeMother::read();
        $replacement = NodeMother::read();

        static::assertSame($replacement, (new ReplaceLeaf($target, $replacement))->of($target));
    }

    public function test_any_other_node_is_returned_unchanged(): void
    {
        $other = NodeMother::select(NodeMother::read());

        static::assertSame($other, (new ReplaceLeaf(NodeMother::read(), NodeMother::read()))->of($other));
    }
}
