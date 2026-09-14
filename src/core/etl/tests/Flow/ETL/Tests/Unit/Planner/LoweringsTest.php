<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Planner\Lowering\LimitLowering;
use Flow\ETL\Planner\Lowering\ReadLowering;
use Flow\ETL\Planner\Lowerings;
use Flow\ETL\Tests\Double\EmptyLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class LoweringsTest extends FlowTestCase
{
    public function test_of_returns_the_lowering_for_a_node(): void
    {
        $lowerings = Lowerings::default();

        static::assertInstanceOf(ReadLowering::class, $lowerings->of(NodeMother::read()));
        static::assertInstanceOf(LimitLowering::class, $lowerings->of(NodeMother::limit(NodeMother::read(), 5)));
    }

    public function test_a_later_registration_for_the_same_kind_wins(): void
    {
        $replacement = new EmptyLowering(Limit::class);

        static::assertSame(
            $replacement,
            (new Lowerings(new LimitLowering(), $replacement))->of(NodeMother::limit(NodeMother::read(), 5)),
        );
    }

    public function test_of_throws_node_not_lowerable_for_an_unregistered_kind(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('No Lowering registered for node ' . Read::class);

        (new Lowerings(new LimitLowering()))->of(NodeMother::read());
    }
}
