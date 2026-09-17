<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\JoinsFrame;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Select;
use Flow\ETL\Plan\Rewrite;
use Flow\ETL\Plan\TransformUp;
use Flow\ETL\Tests\Double\RenameSelectRewrite;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

final class TransformUpTest extends FlowTestCase
{
    public function test_children_are_rewritten_before_their_parent(): void
    {
        $visited = [];

        (new TransformUp())->of(NodeMother::limit(NodeMother::select(NodeMother::read()), 5), new class(
            $visited,
        ) implements Rewrite {
            /**
             * @param list<class-string<Node>> $visited
             */
            public function __construct(
                private array &$visited,
            ) {}

            public function of(Node $node): Node
            {
                $this->visited[] = $node::class;

                return $node;
            }
        });

        static::assertSame([Read::class, Select::class, Limit::class], $visited);
    }

    public function test_a_rewrite_that_changes_nothing_returns_the_same_root_object(): void
    {
        $root = NodeMother::limit(NodeMother::select(NodeMother::read()), 5);

        static::assertSame($root, (new TransformUp())->of($root, new class implements Rewrite {
            public function of(Node $node): Node
            {
                return $node;
            }
        }));
    }

    public function test_one_instance_rewrites_a_node_once_however_often_it_is_reached(): void
    {
        $up = new TransformUp();
        $select = NodeMother::select(NodeMother::read());
        $rewrite = new RenameSelectRewrite();

        $first = $up->of($select, $rewrite);

        static::assertNotSame($select, $first);
        static::assertSame($first, $up->of($select, $rewrite));
    }

    public function test_a_parent_sees_its_rewritten_child(): void
    {
        $rewritten = (new TransformUp())->of(
            NodeMother::limit(NodeMother::select(NodeMother::read()), 5),
            new RenameSelectRewrite(),
        );

        static::assertInstanceOf(Limit::class, $rewritten);
        $select = $rewritten->children()[0];
        static::assertInstanceOf(Select::class, $select);
        static::assertSame(['name'], $select->entries);
    }

    /**
     * @return Generator<string, array{JoinsFrame}>
     */
    public static function joins_sharing_a_node(): Generator
    {
        $select = NodeMother::select(NodeMother::read());

        yield 'join' => [NodeMother::join($select, new Result($select))];

        $select = NodeMother::select(NodeMother::read());

        yield 'cross join' => [NodeMother::crossJoin($select, new Result($select))];
    }

    #[DataProvider('joins_sharing_a_node')]
    public function test_a_joins_right_side_is_handed_back_untouched_even_when_it_shares_a_node_with_the_left(JoinsFrame $join): void
    {
        $rewritten = (new TransformUp())->of($join, new RenameSelectRewrite());

        static::assertInstanceOf(JoinsFrame::class, $rewritten);
        $left = $rewritten->children()[0];
        static::assertInstanceOf(Select::class, $left);
        static::assertSame(['name'], $left->entries);
        static::assertSame($join->right(), $rewritten->right());
    }
}
