<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner;

use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Planner\LimitWalk;
use Flow\ETL\Tests\Double\ChildlessNode;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

final class LimitWalkTest extends FlowTestCase
{
    /**
     * null means "do not push": either no limit survived, or the chain does not end at the leaf.
     */
    #[DataProvider('walks')]
    public function test_the_limit_one_root_lets_through_to_the_leaf(Node $from, Read $leaf, ?int $expected): void
    {
        static::assertSame($expected, (new LimitWalk())->of($from, $leaf));
    }

    public static function walks(): Generator
    {
        $read = NodeMother::read();

        yield 'a limit above the read' => [NodeMother::limit($read, 3), $read, 3];
        yield 'two limits fold with min' => [NodeMother::limit(NodeMother::limit($read, 5), 3), $read, 3];
        yield 'a blocker discards the limit above it' => [
            NodeMother::limit(NodeMother::sort(NodeMother::limit($read, 5)), 3),
            $read,
            5,
        ];
        yield 'an opaque transform discards the limit above it' => [
            NodeMother::limit(new Node\Transform($read, new AddRowIndexTransformer('idx', StartFrom::ZERO)), 3),
            $read,
            null,
        ];
        yield 'a chain ending at a childless node that is not the leaf' => [
            NodeMother::limit(new ChildlessNode(), 3),
            $read,
            null,
        ];
        yield 'a row index discards the limit above it' => [
            NodeMother::limit(new Node\RowIndex($read, 'idx', StartFrom::ZERO), 3),
            $read,
            null,
        ];
        yield 'a limit below a row index passes' => [
            new Node\RowIndex(NodeMother::limit($read, 3), 'idx', StartFrom::ZERO),
            $read,
            3,
        ];
        yield 'the leaf alone' => [$read, $read, null];
    }
}
