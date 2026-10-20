<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\ReplaceNodes;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use SplObjectStorage;

use function Flow\ETL\DSL\lit;

final class ReplaceNodesTest extends FlowTestCase
{
    public function test_a_target_is_replaced_and_any_other_node_returned_as_rebuilt(): void
    {
        $target = NodeMother::read();
        $replacement = NodeMother::read();
        $other = NodeMother::select(NodeMother::read());
        $rewrite = ReplaceNodes::one($target, $replacement);

        static::assertSame($replacement, $rewrite->of($target, $target));
        static::assertSame($other, $rewrite->of($other, $other));
    }

    public function test_a_replacement_takes_the_targets_rewritten_children(): void
    {
        $target = NodeMother::select(NodeMother::read());
        $rebuilt = $target->withChildren([$read = NodeMother::read()]);

        $replaced = ReplaceNodes::one($target, NodeMother::select($target->children()[0], 'name'))->of(
            $target,
            $rebuilt,
        );

        static::assertSame([$read], $replaced->children());
    }

    public function test_a_removed_node_gives_way_to_its_rewritten_child(): void
    {
        $target = new Filter(NodeMother::read(), lit(true));
        $rebuilt = $target->withChildren([$read = NodeMother::read()]);

        /** @var SplObjectStorage<Node, Node> $replacements */
        $replacements = new SplObjectStorage();

        static::assertSame($read, (new ReplaceNodes($replacements, [$target]))->of($target, $rebuilt));
    }

    public function test_a_target_replaced_by_its_own_child_keeps_the_childs_rewritten_children(): void
    {
        $child = NodeMother::select(NodeMother::read());
        $target = new Filter($child, lit(true));
        $rebuilt = $target->withChildren([NodeMother::select(NodeMother::read())]);

        static::assertSame($rebuilt->children(), ReplaceNodes::one($target, $child)->of($target, $rebuilt)->children());
    }

    public function test_a_node_of_other_than_one_child_is_not_removed(): void
    {
        $target = NodeMother::read();
        /** @var SplObjectStorage<Node, Node> $replacements */
        $replacements = new SplObjectStorage();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Only a node of one child gives way to it, ' . $target::class . ' has 0');

        (new ReplaceNodes($replacements, [$target]))->of($target, $target);
    }

    public function test_a_node_both_replaced_and_removed_is_refused(): void
    {
        $target = new Filter(NodeMother::read(), lit(true));
        /** @var SplObjectStorage<Node, Node> $replacements */
        $replacements = new SplObjectStorage();
        $replacements[$target] = NodeMother::read();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A node is either replaced or removed, ' . Filter::class . ' is both');

        new ReplaceNodes($replacements, [$target]);
    }

    public function test_several_targets_are_replaced_in_one_rewrite(): void
    {
        $first = NodeMother::read();
        $second = NodeMother::read();
        /** @var SplObjectStorage<Node, Node> $replacements */
        $replacements = new SplObjectStorage();
        $replacements[$first] = $firstReplacement = NodeMother::read();
        $replacements[$second] = $secondReplacement = NodeMother::read();
        $rewrite = new ReplaceNodes($replacements);

        static::assertSame($firstReplacement, $rewrite->of($first, $first));
        static::assertSame($secondReplacement, $rewrite->of($second, $second));
    }
}
