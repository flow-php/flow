<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner;

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
use Flow\ETL\Planner\FilterWalk;
use Flow\ETL\Tests\Double\RedefiningNode;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\WithEntry;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\rename_replace;

final class FilterWalkTest extends FlowTestCase
{
    public function test_a_root_that_consumes_through_the_filter_is_reached(): void
    {
        $filter = new Filter(NodeMother::read(), lit(true));

        static::assertTrue((new FilterWalk())->reachedByEveryRoot($filter, NodeMother::select($filter), $filter));
    }

    public function test_a_root_that_bypasses_the_filter_is_not_reached(): void
    {
        $read = NodeMother::read();
        $filter = new Filter($read, lit(true));

        static::assertFalse((new FilterWalk())->reachedByEveryRoot(
            $filter,
            NodeMother::select($filter),
            NodeMother::limit($read, 5),
        ));
    }

    public function test_a_chain_that_never_reaches_the_filter_is_not_reached(): void
    {
        $filter = new Filter(NodeMother::read(), lit(true));

        static::assertFalse((new FilterWalk())->reachedByEveryRoot($filter, NodeMother::select(NodeMother::read())));
    }

    public function test_a_transparent_preserving_chain_reaches_the_leaf(): void
    {
        $read = NodeMother::read();

        static::assertTrue((new FilterWalk())->reaches(
            new Filter(NodeMother::select($read), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_another_filter_below_is_transparent(): void
    {
        $read = NodeMother::read();

        static::assertTrue((new FilterWalk())->reaches(
            new Filter(new Filter($read, lit(true)), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_a_limit_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertFalse((new FilterWalk())->reaches(
            new Filter(NodeMother::limit($read, 5), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_an_offset_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertFalse((new FilterWalk())->reaches(
            new Filter(new Offset($read, 5), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_an_until_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertFalse((new FilterWalk())->reaches(
            new Filter(new Until($read, lit(true)), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_a_distinct_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertFalse((new FilterWalk())->reaches(
            new Filter(new Distinct($read, ['id']), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_a_discard_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertFalse((new FilterWalk())->reaches(
            new Filter(new Discard($read), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_an_opaque_node_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertFalse((new FilterWalk())->reaches(
            new Filter(NodeMother::sort($read), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_a_with_column_redefining_a_referenced_name_blocks(): void
    {
        $read = NodeMother::read();

        static::assertFalse((new FilterWalk())->reaches(
            new Filter(new WithColumn($read, 'year', lit(1)), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_a_with_column_over_a_definition_redefining_a_referenced_name_blocks(): void
    {
        $read = NodeMother::read();

        static::assertFalse((new FilterWalk())->reaches(
            new Filter(new WithColumn($read, int_schema('year'), lit(1)), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_a_with_column_over_another_name_does_not_block(): void
    {
        $read = NodeMother::read();

        static::assertTrue((new FilterWalk())->reaches(
            new Filter(new WithColumn($read, 'other', lit(1)), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_a_rename_to_a_referenced_name_blocks(): void
    {
        $read = NodeMother::read();

        static::assertFalse((new FilterWalk())->reaches(
            new Filter(new Rename($read, 'id', 'year'), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_a_rename_to_another_name_does_not_block(): void
    {
        $read = NodeMother::read();

        static::assertTrue((new FilterWalk())->reaches(
            new Filter(new Rename($read, 'id', 'other'), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_a_rename_each_always_blocks(): void
    {
        $read = NodeMother::read();

        static::assertFalse((new FilterWalk())->reaches(
            new Filter(new RenameEach($read, [rename_replace('_', '-')]), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_a_chain_that_does_not_end_in_the_leaf_does_not_reach(): void
    {
        static::assertFalse((new FilterWalk())->reaches(
            new Filter(NodeMother::read(), lit(true)),
            NodeMother::read(),
            refs('year'),
        ));
    }

    public function test_a_duplicate_row_defining_a_referenced_name_blocks(): void
    {
        $read = NodeMother::read();

        static::assertFalse((new FilterWalk())->reaches(new Filter(new DuplicateRow($read, lit(true), [new WithEntry(
            'year',
            lit(2023),
        )]), lit(true)), $read, refs('year')));
    }

    public function test_a_duplicate_row_defining_another_name_does_not_block(): void
    {
        $read = NodeMother::read();

        static::assertTrue((new FilterWalk())->reaches(new Filter(new DuplicateRow($read, lit(true), [new WithEntry(
            'copy',
            lit(1),
        )]), lit(true)), $read, refs('year')));
    }

    public function test_a_node_redefining_a_referenced_column_blocks(): void
    {
        $read = NodeMother::read();

        static::assertFalse((new FilterWalk())->reaches(
            new Filter(new RedefiningNode($read, Redefined::names('year')), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_a_node_redefining_another_column_does_not_block(): void
    {
        $read = NodeMother::read();

        static::assertTrue((new FilterWalk())->reaches(
            new Filter(new RedefiningNode($read, Redefined::names('other')), lit(true)),
            $read,
            refs('year'),
        ));
    }

    public function test_an_unknown_redefinition_blocks(): void
    {
        $read = NodeMother::read();

        static::assertFalse((new FilterWalk())->reaches(
            new Filter(new RedefiningNode($read, Redefined::unknown()), lit(true)),
            $read,
            refs('year'),
        ));
    }
}
