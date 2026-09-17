<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Explain;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Explain\Details;
use Flow\ETL\Plan\Node\Collect;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Offset;
use Flow\ETL\Plan\Node\Rename;
use Flow\ETL\Plan\Node\RenameEach;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\TopN;
use Flow\ETL\Plan\Node\Until;
use Flow\ETL\Plan\Node\WithColumn;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\Filesystem\Path\Filter\KeepAll;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\rename_replace;
use function Flow\ETL\DSL\to_memory;

final class DetailsTest extends FlowTestCase
{
    public function test_name_is_the_node_class_short_name(): void
    {
        static::assertSame('Select', (new Details())->name(NodeMother::select(NodeMother::read())));
    }

    public function test_a_read_lists_its_extractor_only_when_nothing_was_pushed_into_it(): void
    {
        static::assertSame(['Extractor: ArrayExtractor'], (new Details())->lines(NodeMother::read()));
    }

    public function test_a_read_lists_a_pushed_limit_and_file_filter(): void
    {
        static::assertSame(
            ['Extractor: ArrayExtractor', 'Limit: 3', 'Files: Filters'],
            (new Details())->lines(NodeMother::read()->withLimit(3)->withPathFilter(new KeepAll())),
        );
    }

    public function test_nodes_with_arguments_label_them(): void
    {
        $read = NodeMother::read();
        $details = new Details();

        static::assertSame(['Condition: IsNotNull'], $details->lines(new Filter($read, ref('id')->isNotNull())));
        static::assertSame(['Until: Literal'], $details->lines(new Until($read, lit(true))));
        static::assertSame(['Loader: MemoryLoader'], $details->lines(new Write($read, to_memory(new ArrayMemory()))));
        static::assertSame(['Limit: 5'], $details->lines(NodeMother::limit($read, 5)));
        static::assertSame(['Skip: 2'], $details->lines(new Offset($read, 2)));
        static::assertSame(
            ['Top: 4', 'Buffers all rows before passing them on'],
            $details->lines(new TopN($read, refs(ref('id')), 4)),
        );
    }

    public function test_a_result_says_what_it_returns(): void
    {
        static::assertSame(
            ['Rows this plan hands out: to the trigger, or to the node reading it'],
            (new Details())->lines(new Result(NodeMother::read())),
        );
    }

    public function test_a_node_without_arguments_has_no_details(): void
    {
        static::assertSame([], (new Details())->lines(NodeMother::select(NodeMother::read())));
    }

    public function test_a_blocking_node_says_it_buffers_rows(): void
    {
        static::assertSame(
            ['Buffers all rows before passing them on'],
            (new Details())->lines(new Collect(NodeMother::read())),
        );
    }

    public function test_redefined_columns_follow_the_labelled_details(): void
    {
        $read = NodeMother::read();
        $details = new Details();

        static::assertSame(
            ['Column: n = Literal', 'Defines columns: n'],
            $details->lines(new WithColumn($read, int_schema('n'), lit(1))),
        );
        static::assertSame(['Rename: n → m', 'Defines columns: m'], $details->lines(new Rename($read, 'n', 'm')));
        static::assertSame(
            ['Defines columns known only at run time'],
            $details->lines(new RenameEach($read, [rename_replace('_', '-')])),
        );
    }

    public function test_declarations_are_the_row_count_transparency_materialization_and_redefinitions(): void
    {
        $read = NodeMother::read();
        $details = new Details();

        static::assertSame('preserving · transparent · blocking', $details->declarations(new Collect($read)));
        static::assertSame(
            'preserving · transparent · streaming · redefines m',
            $details->declarations(new Rename($read, 'n', 'm')),
        );
        static::assertSame('preserving · transparent · streaming · redefines unknown', $details->declarations(
            new RenameEach($read, [rename_replace('_', '-')]),
        ));
    }

    public function test_notes_leave_out_the_labelled_details(): void
    {
        $read = NodeMother::read();
        $details = new Details();

        static::assertSame([], $details->notes(NodeMother::limit($read, 5)));
        static::assertSame(
            ['Buffers all rows before passing them on'],
            $details->notes(new TopN($read, refs(ref('id')), 4)),
        );
        static::assertSame(['Top: 4'], $details->labelled(new TopN($read, refs(ref('id')), 4)));
    }

    public function test_name_of_any_object_drops_the_namespace(): void
    {
        static::assertSame('KeepAll', (new Details())->name(new KeepAll()));
    }
}
