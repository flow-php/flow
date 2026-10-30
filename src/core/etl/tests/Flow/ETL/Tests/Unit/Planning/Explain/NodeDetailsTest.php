<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Explain;

use Flow\ETL\Functions\Join\Condition\Equal;
use Flow\ETL\Functions\Join\Expression;
use Flow\ETL\IO\Filesystem\ScalarFunctionFilter;
use Flow\ETL\Operators\Processor\RequiredColumns;
use Flow\ETL\Planning\Explain\NodeDetails;
use Flow\ETL\Planning\Logical\Node\Collect;
use Flow\ETL\Planning\Logical\Node\CollectRefs;
use Flow\ETL\Planning\Logical\Node\CrossJoin;
use Flow\ETL\Planning\Logical\Node\ExpandColumn;
use Flow\ETL\Planning\Logical\Node\Filter;
use Flow\ETL\Planning\Logical\Node\Join as JoinNode;
use Flow\ETL\Planning\Logical\Node\Offset;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\Node\Rename;
use Flow\ETL\Planning\Logical\Node\RenameEach;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Node\TopN;
use Flow\ETL\Planning\Logical\Node\Until;
use Flow\ETL\Planning\Logical\Node\WithColumn;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\PartitionedSourceMother;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\NullsOrder;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\hash_join;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\rename_replace;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\to_memory;

final class NodeDetailsTest extends FlowTestCase
{
    public function test_a_sort_labels_its_columns_with_the_direction_and_a_nulls_order_that_is_not_the_default(): void
    {
        static::assertSame(
            ['Sort by: id desc, name asc nulls last'],
            (new NodeDetails())->labelled(NodeMother::sort(NodeMother::read(), [
                ref('id')->desc(),
                ref('name')->asc(NullsOrder::LAST),
            ])),
        );
    }

    public function test_a_read_lists_its_extractor_only_when_nothing_was_pushed_into_it(): void
    {
        static::assertSame(['Extractor: ArrayExtractor'], (new NodeDetails())->lines(NodeMother::read()));
    }

    public function test_a_read_lists_a_pushed_limit_file_filter_and_each_conjunct(): void
    {
        static::assertSame(
            [
                'Extractor: RecordingFileExtractor',
                'Source: file:///dev/null',
                'Limit: 3',
                'Files: KeepAll',
                'Filter: year > 1',
                'Filter: month IS NOT NULL',
            ],
            (new NodeDetails())->lines(
                (new Read(PartitionedSourceMother::yearMonth()))
                    ->withLimit(3)
                    ->withPathFilter(new KeepAll())
                    ->withFilter(ref('year')->greaterThan(lit(1))->and(ref('month')->isNotNull())),
            ),
        );
    }

    public function test_files_are_each_pushed_file_filter_but_the_default_one(): void
    {
        static::assertSame(
            ["country IN ('PL', 'DE')", 'KeepAll', 'year > 2020'],
            (new NodeDetails())->files(
                new Filters(
                    new OnlyFiles(),
                    new ScalarFunctionFilter(
                        ref('country')->isIn(lit(['PL', 'DE'])),
                        schema(),
                        flow_context(config())->backend(),
                    ),
                    new Filters(
                        new KeepAll(),
                        new ScalarFunctionFilter(
                            ref('year')->greaterThan(lit(2020)),
                            schema(),
                            flow_context(config())->backend(),
                        ),
                    ),
                ),
            ),
        );
        static::assertSame([], (new NodeDetails())->files(new OnlyFiles()));
    }

    public function test_nodes_with_arguments_label_them(): void
    {
        $read = NodeMother::read();
        $details = new NodeDetails();

        static::assertSame(['Condition: id IS NOT NULL'], $details->lines(new Filter($read, ref('id')->isNotNull())));
        static::assertSame(['Until: true'], $details->lines(new Until($read, lit(true))));
        static::assertSame(['Loader: MemoryLoader'], $details->lines(new Write($read, to_memory(new ArrayMemory()))));
        static::assertSame(['Limit: 5'], $details->lines(NodeMother::limit($read, 5)));
        static::assertSame(['Skip: 2'], $details->lines(new Offset($read, 2)));
        static::assertSame(['Top: 4'], $details->lines(new TopN($read, [ref('id')->asc()], 4)));
    }

    public function test_a_join_labels_its_type_and_condition(): void
    {
        static::assertSame(
            [
                'Type: left',
                'On: id = id',
                'Changes columns known only at run time',
            ],
            (new NodeDetails())->lines(NodeMother::join(
                NodeMother::read(),
                NodeMother::joinRight(NodeMother::plan(NodeMother::read())),
            )),
        );
    }

    public function test_a_join_labels_a_prefix_and_an_algorithm_only_when_they_were_given(): void
    {
        static::assertSame(
            [
                'Type: right',
                'On: id = user_id',
                'On: tag = tag',
                'Prefix: joined_',
                'Algorithm: HashJoin',
            ],
            (new NodeDetails())->labelled(
                new JoinNode(
                    NodeMother::read(),
                    NodeMother::joinRight(NodeMother::plan(NodeMother::read())),
                    Expression::on([new Equal('id', 'user_id'), new Equal('tag', 'tag')], 'joined_'),
                    JoinType::right,
                    hash_join(),
                ),
            ),
        );
    }

    public function test_a_cross_join_labels_its_prefix_only_when_it_was_given(): void
    {
        $details = new NodeDetails();
        $right = NodeMother::joinRight(NodeMother::plan(NodeMother::read()));

        static::assertSame(['Type: cross'], $details->labelled(new CrossJoin(NodeMother::read(), $right)));
        static::assertSame(
            ['Type: cross', 'Prefix: r_'],
            $details->labelled(new CrossJoin(NodeMother::read(), $right, 'r_')),
        );
    }

    public function test_a_result_says_what_it_returns(): void
    {
        static::assertSame(
            ['Rows this plan hands out: to the trigger, or to the node reading it'],
            (new NodeDetails())->lines(new Result(NodeMother::read())),
        );
    }

    public function test_a_node_without_arguments_has_no_details(): void
    {
        static::assertSame([], (new NodeDetails())->lines(new CollectRefs(NodeMother::read(), refs())));
    }

    public function test_an_expand_column_shows_the_columns_it_keeps(): void
    {
        $read = NodeMother::read();
        $details = new NodeDetails();
        $expand = new ExpandColumn($read, 'item', ref('items')->expand());

        static::assertSame(['Column: item = ArrayExpand(items)'], $details->labelled($expand));
        static::assertSame(
            ['Column: item = ArrayExpand(items)', 'Keeps: id, item'],
            $details->labelled($expand->withCarries(RequiredColumns::only('id', 'item'))),
        );
        static::assertSame(
            ['Column: item = ArrayExpand(items)', 'Keeps: no input column'],
            $details->labelled($expand->withCarries(RequiredColumns::only())),
        );
        static::assertSame(
            ['Column: item = ArrayExpand(items)', 'Keeps: every column but items, body'],
            $details->labelled($expand->withCarries(RequiredColumns::allBut('items', 'body'))),
        );
    }

    public function test_redefined_columns_follow_the_labelled_details(): void
    {
        $read = NodeMother::read();
        $details = new NodeDetails();

        static::assertSame(
            ['Column: n = 1', 'Changes columns: n'],
            $details->lines(new WithColumn($read, int_schema('n'), lit(1))),
        );
        static::assertSame(['Rename: n → m', 'Changes columns: m, n'], $details->lines(new Rename($read, 'n', 'm')));
        static::assertSame(
            ['Changes columns known only at run time'],
            $details->lines(new RenameEach($read, [rename_replace('_', '-')])),
        );
    }

    public function test_declarations_are_the_row_count_transparency_and_redefinitions(): void
    {
        $read = NodeMother::read();
        $details = new NodeDetails();

        static::assertSame('preserving · transparent', $details->declarations(new Collect($read)));
        static::assertSame(
            'preserving · transparent · redefines m, n',
            $details->declarations(new Rename($read, 'n', 'm')),
        );
        static::assertSame('preserving · transparent · redefines unknown', $details->declarations(
            new RenameEach($read, [rename_replace('_', '-')]),
        ));
    }

    public function test_notes_leave_out_the_labelled_details(): void
    {
        $read = NodeMother::read();
        $details = new NodeDetails();

        static::assertSame([], $details->notes(NodeMother::limit($read, 5)));
        static::assertSame([], $details->notes(new TopN($read, [ref('id')->asc()], 4)));
        static::assertSame(['Top: 4'], $details->labelled(new TopN($read, [ref('id')->asc()], 4)));
    }
}
