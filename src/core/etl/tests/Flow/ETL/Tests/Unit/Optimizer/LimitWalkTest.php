<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Optimizer;

use Flow\ETL\Optimizer\LimitWalk;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Tests\Double\ChildlessNode;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

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
        yield 'a limit below an opaque transform passes' => [
            new Node\Transform(NodeMother::limit($read, 3), new AddRowIndexTransformer('idx', StartFrom::ZERO)),
            $read,
            3,
        ];
        yield 'a limit above an offset grows by the skipped rows' => [
            NodeMother::limit(new Node\Offset($read, 100), 10),
            $read,
            110,
        ];
        yield 'an offset above a limit leaves the limit' => [new Node\Offset(NodeMother::limit($read, 5), 2), $read, 5];
        yield 'an offset without a limit pushes nothing' => [new Node\Offset($read, 100), $read, null];
        yield 'a top-n discards the limit above it' => [
            NodeMother::limit(new Node\TopN($read, refs(ref('id')), 5), 3),
            $read,
            null,
        ];
        yield 'the leaf alone' => [$read, $read, null];
    }
}
