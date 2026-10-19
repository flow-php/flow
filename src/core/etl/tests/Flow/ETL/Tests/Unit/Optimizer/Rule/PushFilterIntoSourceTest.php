<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Optimizer\Rule;

use DateTimeImmutable;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Function\All;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Optimizer;
use Flow\ETL\Optimizer\Rule\PushFilterIntoSource;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Stage;
use Flow\ETL\Tests\Double\FixedRandomValueGenerator;
use Flow\ETL\Tests\Double\RecordingExtractor;
use Flow\ETL\Tests\Double\RecordingFileExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\PartitionedSourceMother;
use Flow\ETL\WithEntry;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\random_string;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\to_memory;
use function Flow\Types\DSL\type_boolean;

final class PushFilterIntoSourceTest extends FlowTestCase
{
    public function test_a_partition_only_predicate_is_pushed_into_the_source(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        $rows = df()
            ->read($extractor)
            ->filter(ref('year')->equals(lit(2023)))
            ->fetch();

        $pathFilter = $extractor->scans[0]->pathFilter;
        static::assertInstanceOf(Filters::class, $pathFilter);
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        // the double lists every partition, so only the scan's filter drops 2024
        static::assertSame([2023], $rows->reduceToArray('year'));
    }

    public function test_two_stacked_partition_filters_are_both_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->filter(ref('year')->equals(lit(2023)))
            ->filter(ref('month')->equals(lit('07')))
            ->fetch();

        $pathFilter = $extractor->scans[0]->pathFilter;
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=08')));
    }

    public function test_three_stacked_partition_filters_all_reach_the_source(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->filter(ref('year')->greaterThanEqual(lit(2023)))
            ->filter(ref('year')->lessThanEqual(lit(2023)))
            ->filter(ref('month')->equals(lit('07')))
            ->fetch();

        $pathFilter = $extractor->scans[0]->pathFilter;
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2022/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=08')));
    }

    public function test_a_body_predicate_below_a_partition_filter_does_not_block_it(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        $rows = df()
            ->read($extractor)
            ->filter(ref('value')->notEquals(lit('')))
            ->filter(ref('year')->equals(lit(2023)))
            ->fetch();

        $pathFilter = $extractor->scans[0]->pathFilter;
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        static::assertSame(['a'], $rows->reduceToArray('value'));
    }

    public function test_a_body_column_predicate_is_pushed_as_a_filter_and_never_as_a_path_filter(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->filter($predicate = ref('value')->equals(lit('a')))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->scans[0]->pathFilter);
        static::assertSame($predicate, $extractor->scans[0]->filter);
    }

    public function test_a_mixed_predicate_pushes_both_conjuncts_and_only_the_partition_one_as_a_path_filter(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        $rows = df()
            ->read($extractor)
            ->filter(($year = ref('year')->equals(lit(2023)))->and($value = ref('value')->notEquals(lit('a'))))
            ->fetch();

        $pathFilter = $extractor->scans[0]->pathFilter;
        static::assertInstanceOf(Filters::class, $pathFilter);
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        static::assertEquals(new All($year, $value), $extractor->scans[0]->filter);
        // the scan applies the body conjunct too: the only 2023 row has value "a"
        static::assertCount(0, $rows);
    }

    public function test_a_mixed_predicate_with_two_partition_conjuncts_pushes_both(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->filter(
                ref('year')
                    ->equals(lit(2023))
                    ->and(ref('month')->equals(lit('07')))
                    ->and(ref('value')->notEquals(lit(''))),
            )
            ->fetch();

        $pathFilter = $extractor->scans[0]->pathFilter;
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=08')));
    }

    public function test_a_conjunct_that_is_not_deterministic_is_skipped_and_the_rest_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->filter(
                ref('year')
                    ->equals(lit(2023))
                    ->and(ref('month')->equals(random_string(2, new FixedRandomValueGenerator('xx')))),
            )
            ->fetch();

        $pathFilter = $extractor->scans[0]->pathFilter;
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
    }

    public function test_an_or_over_partition_columns_is_pushed_whole(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->filter(ref('year')->equals(lit(2023))->or(ref('year')->equals(lit(2024))))
            ->fetch();

        $pathFilter = $extractor->scans[0]->pathFilter;
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2025/month=07')));
    }

    public function test_an_or_mixing_a_body_column_is_pushed_whole_as_a_filter_and_never_as_a_path_filter(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->filter($predicate = ref('year')->equals(lit(2023))->or(ref('value')->equals(lit('x'))))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->scans[0]->pathFilter);
        static::assertSame($predicate, $extractor->scans[0]->filter);
    }

    public function test_a_nested_and_inside_an_and_pushes_the_partition_conjuncts(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        // and() on a plain function wraps its argument, so the partition pair stays one nested All
        $rows = df()
            ->read($extractor)
            ->filter(
                ref('value')
                    ->notEquals(lit(''))
                    ->and(ref('year')->equals(lit(2023))->and(ref('month')->equals(lit('07')))),
            )
            ->fetch();

        $pathFilter = $extractor->scans[0]->pathFilter;
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=08')));
        static::assertSame(['a'], $rows->reduceToArray('value'));
    }

    public function test_a_limit_below_the_filter_blocks_the_push(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->limit(5)
            ->filter(ref('year')->equals(lit(2023)))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->scans[0]->pathFilter);
    }

    public function test_a_with_entry_redefining_the_partition_column_blocks_the_push(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->withEntry('year', lit(2023))
            ->filter(ref('year')->equals(lit(2023)))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->scans[0]->pathFilter);
    }

    public function test_a_duplicate_row_defining_the_partition_column_blocks_the_push(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        $rows = df()
            ->read($extractor)
            ->duplicateRow(lit(true), new WithEntry('year', lit(2023)))
            ->filter(ref('year')->equals(lit(2023)))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->scans[0]->pathFilter);
        // the 2024 row's duplicate carries year=2023: pruning its partition would lose it
        static::assertCount(3, $rows);
    }

    public function test_a_duplicate_row_defining_another_column_does_not_block_the_push(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->duplicateRow(lit(true), new WithEntry('copy', lit(1)))
            ->filter(ref('year')->equals(lit(2023)))
            ->fetch();

        static::assertInstanceOf(Filters::class, $extractor->scans[0]->pathFilter);
    }

    public function test_a_filter_a_sink_root_does_not_consume_through_is_not_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->write(to_memory(new ArrayMemory()))
            ->filter(ref('year')->equals(lit(2023)))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->scans[0]->pathFilter);
    }

    public function test_a_non_file_extractor_is_never_pushed_into(): void
    {
        $plan = NodeMother::plan(
            new Filter(new Read(new RecordingExtractor(schema(int_schema('year')))), ref('year')->equals(lit(2023))),
        );

        static::assertSame($plan, (new PushFilterIntoSource())->apply($plan, NodeMother::context()));
    }

    public function test_a_data_conjunct_is_pushed_into_a_source_without_partition_columns(): void
    {
        $extractor = new RecordingFileExtractor(
            schema(int_schema('year')),
            array_to_rows([['year' => 2023]], schema(int_schema('year'))),
        );

        df()
            ->read($extractor)
            ->filter($predicate = ref('year')->equals(lit(2023)))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->scans[0]->pathFilter);
        static::assertSame($predicate, $extractor->scans[0]->filter);
    }

    public function test_a_non_deterministic_data_conjunct_is_not_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->filter(ref('value')->equals(random_string(2)))
            ->fetch();

        static::assertNull($extractor->scans[0]->filter);
    }

    public function test_a_data_conjunct_under_a_reducing_node_is_not_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->limit(5)
            ->filter(ref('value')->equals(lit('a')))
            ->fetch();

        static::assertSame(5, $extractor->scans[0]->limit);
        static::assertNull($extractor->scans[0]->filter);
    }

    public function test_a_non_deterministic_predicate_is_not_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->filter(ref('month')->equals(random_string(2)))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->scans[0]->pathFilter);
    }

    public function test_a_predicate_calling_a_user_callable_is_not_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->filter(ref('year')->call(lit('is_string'), type_boolean()))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->scans[0]->pathFilter);
    }

    public function test_a_literal_only_predicate_is_not_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()->read($extractor)->filter(lit(true))->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->scans[0]->pathFilter);
    }

    public function test_a_filter_inside_a_joined_frame_is_pushed_with_that_frames_context(): void
    {
        $right = PartitionedSourceMother::yearMonth();

        df()
            ->read(from_array([['id' => 2023]]))
            ->join(df()->read($right)->filter(ref('year')->equals(lit(2023))), join_on(['id' => 'year'], 'r_'))
            ->fetch();

        $pathFilter = $right->scans[0]->pathFilter;
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
    }

    public function test_a_string_partition_compared_with_a_date_is_cast_and_a_non_date_value_fails(): void
    {
        $this->expectException(EvaluationException::class);
        $this->expectExceptionMessage('Cast function failed: Can\'t cast "string" into "date" type (row 0)');

        df()
            ->read(PartitionedSourceMother::yearMonth())
            ->filter(ref('month')->equals(lit(new DateTimeImmutable('2024-01-01'))))
            ->fetch();
    }

    public function test_the_extractor_instance_is_never_mutated(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();
        $frame = df()->read($extractor)->filter(ref('year')->equals(lit(2023)));

        $frame->fetch();

        static::assertStringEndsWith(
            "Extractor: RecordingFileExtractor\n         Source: file:///dev/null",
            $frame->explain()->toString(Stage::unoptimized),
        );
        static::assertStringEndsWith(
            "Extractor: RecordingFileExtractor\n      Source: file:///dev/null\n      Files: Filters\n      Filter: Equals",
            $frame->explain()->toString(),
        );
    }

    public function test_a_filter_on_a_renamed_partition_column_is_pushed_as_the_original_column(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        $rows = df()
            ->read($extractor)
            ->rename('year', 'y')
            ->filter(ref('y')->equals(lit(2023)))
            ->fetch();

        $pathFilter = $extractor->scans[0]->pathFilter;
        static::assertInstanceOf(Filters::class, $pathFilter);
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        static::assertSame([2023], $rows->reduceToArray('y'));
    }

    public function test_a_filter_on_an_alias_of_a_partition_column_is_pushed_as_the_original_column(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        $rows = df()
            ->read($extractor)
            ->withEntry('y', ref('year'))
            ->filter(ref('y')->equals(lit(2023)))
            ->fetch();

        $pathFilter = $extractor->scans[0]->pathFilter;
        static::assertInstanceOf(Filters::class, $pathFilter);
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        static::assertSame([2023], $rows->reduceToArray('y'));
    }

    public function test_a_filter_on_an_alias_of_a_body_column_is_pushed_as_the_original_column_and_never_as_a_path_filter(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->withEntry('v', ref('value'))
            ->filter(ref('v')->equals($literal = lit('a')))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->scans[0]->pathFilter);
        static::assertEquals(ref('value')->equals($literal), $extractor->scans[0]->filter);
    }

    public function test_a_filter_node_whose_only_conjunct_is_pushed_is_removed(): void
    {
        $plan = NodeMother::plan(
            new Filter(new Read(PartitionedSourceMother::yearMonth()), $year = ref('year')->equals(lit(2023))),
        );

        $read = (new PushFilterIntoSource())->apply($plan, NodeMother::context())->root->children()[0];

        static::assertInstanceOf(Read::class, $read);
        static::assertSame($year, $read->scan()->filter);
    }

    public function test_a_pushed_conjunct_leaves_its_filter_node_and_the_rest_stays_in_it(): void
    {
        $plan = NodeMother::plan(
            new Filter(
                new Read(PartitionedSourceMother::yearMonth()),
                ($year = ref('year')->equals(lit(2023)))->and($random = ref('value')->equals(random_string(2))),
            ),
        );

        $filter = (new PushFilterIntoSource())->apply($plan, NodeMother::context())->root->children()[0];

        static::assertInstanceOf(Filter::class, $filter);
        static::assertSame($random, $filter->function);
        $read = $filter->children()[0];
        static::assertInstanceOf(Read::class, $read);
        static::assertSame($year, $read->scan()->filter);
    }

    public function test_stacked_filter_nodes_are_pushed_leaf_first(): void
    {
        $plan = NodeMother::plan(
            new Filter(
                new Filter(new Read(PartitionedSourceMother::yearMonth()), $year = ref('year')->equals(lit(2023))),
                $month = ref('month')->equals(lit('07')),
            ),
        );

        $read = (new PushFilterIntoSource())->apply($plan, NodeMother::context())->root->children()[0];

        static::assertInstanceOf(Read::class, $read);
        static::assertEquals(new All($year, $month), $read->scan()->filter);
    }

    public function test_a_filter_node_that_keeps_a_conjunct_stops_the_push_of_the_filters_above_it(): void
    {
        $plan = NodeMother::plan(
            new Filter(
                new Filter(
                    new Read(PartitionedSourceMother::yearMonth()),
                    ($year = ref('year')->equals(lit(2023)))->and($random = ref('value')->equals(random_string(2))),
                ),
                $month = ref('month')->equals(lit('07')),
            ),
        );

        $top = (new PushFilterIntoSource())->apply($plan, NodeMother::context())->root->children()[0];

        static::assertInstanceOf(Filter::class, $top);
        static::assertSame($month, $top->function);
        $bottom = $top->children()[0];
        static::assertInstanceOf(Filter::class, $bottom);
        static::assertSame($random, $bottom->function);
        $read = $bottom->children()[0];
        static::assertInstanceOf(Read::class, $read);
        static::assertSame($year, $read->scan()->filter);
    }

    public function test_a_filter_node_keeping_two_conjuncts_keeps_them_as_one_all(): void
    {
        $plan = NodeMother::plan(
            new Filter(
                new Read(PartitionedSourceMother::yearMonth()),
                ref('year')
                    ->equals(lit(2023))
                    ->and($first = ref('value')->equals(random_string(2)))
                    ->and($second = ref('value')->notEquals(random_string(3))),
            ),
        );

        $filter = (new PushFilterIntoSource())->apply($plan, NodeMother::context())->root->children()[0];

        static::assertInstanceOf(Filter::class, $filter);
        static::assertEquals(new All($first, $second), $filter->function);
    }

    public function test_a_limit_above_a_fully_pushed_filter_is_pushed_beside_it(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        $rows = df()
            ->read($extractor)
            ->filter($year = ref('year')->equals(lit(2023)))
            ->limit(1)
            ->fetch();

        static::assertSame(1, $extractor->scans[0]->limit);
        static::assertSame($year, $extractor->scans[0]->filter);
        static::assertSame([2023], $rows->reduceToArray('year'));
    }

    public function test_a_limit_above_a_filter_node_that_keeps_a_conjunct_is_not_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->filter(ref('year')->equals(lit(2023))->and(ref('value')->equals(random_string(2))))
            ->limit(1)
            ->fetch();

        static::assertNull($extractor->scans[0]->limit);
    }

    public function test_a_kept_conjunct_keeps_every_conjunct_after_it_in_its_node(): void
    {
        $plan = NodeMother::plan(
            new Filter(
                new Read(PartitionedSourceMother::yearMonth()),
                ($year = ref('year')->equals(lit(2023)))->and($random = ref('value')->equals(random_string(2)))->and(
                    $month = ref('month')->equals(lit('07')),
                ),
            ),
        );

        $filter = (new PushFilterIntoSource())->apply($plan, NodeMother::context())->root->children()[0];

        static::assertInstanceOf(Filter::class, $filter);
        static::assertEquals(new All($random, $month), $filter->function);
        $read = $filter->children()[0];
        static::assertInstanceOf(Read::class, $read);
        static::assertSame($year, $read->scan()->filter);
    }
}
