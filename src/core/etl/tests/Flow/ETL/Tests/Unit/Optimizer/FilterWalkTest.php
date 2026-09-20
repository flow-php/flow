<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Optimizer;

use Flow\ETL\Optimizer\FilterWalk;
use Flow\ETL\Plan\Node\Discard;
use Flow\ETL\Plan\Node\Distinct;
use Flow\ETL\Plan\Node\DuplicateRow;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Offset;
use Flow\ETL\Plan\Node\Rename;
use Flow\ETL\Plan\Node\RenameEach;
use Flow\ETL\Plan\Node\Until;
use Flow\ETL\Plan\Node\WithColumn;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Tests\Double\RedefiningNode;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\WithEntry;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rename_replace;

final class FilterWalkTest extends FlowTestCase
{
    public function test_a_consumer_reading_through_the_filter_is_reached(): void
    {
        $filter = new Filter(NodeMother::read(), lit(true));

        static::assertTrue((new FilterWalk())->reachedByEveryConsumer($filter, NodeMother::select($filter), $filter));
    }

    public function test_a_consumer_bypassing_the_filter_is_not_reached(): void
    {
        $read = NodeMother::read();
        $filter = new Filter($read, lit(true));

        static::assertFalse((new FilterWalk())->reachedByEveryConsumer(
            $filter,
            NodeMother::select($filter),
            NodeMother::limit($read, 5),
        ));
    }

    public function test_a_chain_that_never_reaches_the_filter_is_not_reached(): void
    {
        $filter = new Filter(NodeMother::read(), lit(true));

        static::assertFalse((new FilterWalk())->reachedByEveryConsumer(
            $filter,
            NodeMother::select(NodeMother::read()),
        ));
    }

    public function test_a_transparent_preserving_chain_reaches_the_leaf(): void
    {
        $read = NodeMother::read();

        static::assertNotNull((new FilterWalk())->predicateAtLeaf(
            new Filter(NodeMother::select($read), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_another_filter_below_is_transparent(): void
    {
        $read = NodeMother::read();

        static::assertNotNull((new FilterWalk())->predicateAtLeaf(
            new Filter(new Filter($read, lit(true)), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_limit_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new FilterWalk())->predicateAtLeaf(
            new Filter(NodeMother::limit($read, 5), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_an_offset_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new FilterWalk())->predicateAtLeaf(
            new Filter(new Offset($read, 5), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_an_until_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new FilterWalk())->predicateAtLeaf(
            new Filter(new Until($read, lit(true)), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_distinct_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new FilterWalk())->predicateAtLeaf(
            new Filter(new Distinct($read, ['id']), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_discard_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new FilterWalk())->predicateAtLeaf(
            new Filter(new Discard($read), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_an_opaque_node_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new FilterWalk())->predicateAtLeaf(
            new Filter(NodeMother::sort($read), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_with_column_redefining_a_referenced_name_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new FilterWalk())->predicateAtLeaf(
            new Filter(new WithColumn($read, 'year', lit(1)), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_with_column_over_a_definition_redefining_a_referenced_name_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new FilterWalk())->predicateAtLeaf(
            new Filter(new WithColumn($read, int_schema('year'), lit(1)), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_with_column_over_another_name_does_not_block(): void
    {
        $read = NodeMother::read();

        static::assertNotNull((new FilterWalk())->predicateAtLeaf(
            new Filter(new WithColumn($read, 'other', lit(1)), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_rename_to_a_referenced_name_rewrites_the_predicate_to_the_column_below(): void
    {
        $read = NodeMother::read();

        static::assertEquals(
            ref('id')->isNotNull(),
            (new FilterWalk())->predicateAtLeaf(
                new Filter(new Rename($read, 'id', 'year'), lit(true)),
                $read,
                ref('year')->isNotNull(),
            ),
        );
    }

    public function test_a_rename_to_another_name_does_not_block(): void
    {
        $read = NodeMother::read();

        static::assertNotNull((new FilterWalk())->predicateAtLeaf(
            new Filter(new Rename($read, 'id', 'other'), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_rename_each_always_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new FilterWalk())->predicateAtLeaf(
            new Filter(new RenameEach($read, [rename_replace('_', '-')]), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_chain_that_does_not_end_in_the_leaf_does_not_reach(): void
    {
        static::assertNull((new FilterWalk())->predicateAtLeaf(
            new Filter(NodeMother::read(), lit(true)),
            NodeMother::read(),
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_duplicate_row_defining_a_referenced_name_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new FilterWalk())->predicateAtLeaf(new Filter(new DuplicateRow(
            $read,
            lit(true),
            [new WithEntry('year', lit(2023))],
        ), lit(true)), $read, ref('year')->isNotNull()));
    }

    public function test_a_duplicate_row_defining_another_name_does_not_block(): void
    {
        $read = NodeMother::read();

        static::assertNotNull((new FilterWalk())->predicateAtLeaf(new Filter(new DuplicateRow(
            $read,
            lit(true),
            [new WithEntry('copy', lit(1))],
        ), lit(true)), $read, ref('year')->isNotNull()));
    }

    public function test_a_node_redefining_a_referenced_column_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new FilterWalk())->predicateAtLeaf(
            new Filter(new RedefiningNode($read, Redefined::names('year')), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_node_redefining_another_column_does_not_block(): void
    {
        $read = NodeMother::read();

        static::assertNotNull((new FilterWalk())->predicateAtLeaf(
            new Filter(new RedefiningNode($read, Redefined::names('other')), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_an_unknown_redefinition_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new FilterWalk())->predicateAtLeaf(
            new Filter(new RedefiningNode($read, Redefined::unknown()), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_with_column_aliasing_a_column_rewrites_the_predicate_to_that_column(): void
    {
        $read = NodeMother::read();

        static::assertEquals(
            ref('id')->isNotNull(),
            (new FilterWalk())->predicateAtLeaf(
                new Filter(new WithColumn($read, 'year', ref('id')), lit(true)),
                $read,
                ref('year')->isNotNull(),
            ),
        );
    }

    public function test_a_with_column_aliasing_a_column_under_a_definition_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new FilterWalk())->predicateAtLeaf(
            new Filter(new WithColumn($read, int_schema('year'), ref('id')), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_stacked_aliases_rewrite_the_predicate_through_every_one(): void
    {
        $read = NodeMother::read();

        static::assertEquals(
            ref('id')->equals(ref('month')),
            (new FilterWalk())->predicateAtLeaf(
                new Filter(new Rename(new WithColumn($read, 'copy', ref('id')), 'copy', 'year'), lit(true)),
                $read,
                ref('year')->equals(ref('month')),
            ),
        );
    }

    public function test_a_predicate_reading_no_redefined_name_arrives_unchanged(): void
    {
        $read = NodeMother::read();
        $predicate = ref('month')->isNotNull();

        static::assertSame($predicate, (new FilterWalk())->predicateAtLeaf(
            new Filter(new Rename($read, 'id', 'year'), lit(true)),
            $read,
            $predicate,
        ));
    }
}
