<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Optimizer\Rule;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Functions\All;
use Flow\ETL\Functions\WithColumn;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Discard;
use Flow\ETL\Planning\Logical\Node\Distinct;
use Flow\ETL\Planning\Logical\Node\DuplicateRow;
use Flow\ETL\Planning\Logical\Node\Filter;
use Flow\ETL\Planning\Logical\Node\Offset;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\Node\Rename;
use Flow\ETL\Planning\Logical\Node\RenameEach;
use Flow\ETL\Planning\Logical\Node\Until;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\Stage;
use Flow\ETL\Planning\Optimizer\Rule\PushFilterIntoSource;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\FixedRandomValueGenerator;
use Flow\ETL\Tests\Double\RecordingExtractor;
use Flow\ETL\Tests\Double\RecordingFileExtractor;
use Flow\ETL\Tests\Double\RedefiningNode;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\PartitionedSourceMother;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\random_string;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rename_replace;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\with_column;
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

        $pathFilter = $extractor->criteria[0]->pathFilter;
        static::assertInstanceOf(Filters::class, $pathFilter);
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        // the double lists every partition, so only the criteria's filter drops 2024
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

        $pathFilter = $extractor->criteria[0]->pathFilter;
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

        $pathFilter = $extractor->criteria[0]->pathFilter;
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

        $pathFilter = $extractor->criteria[0]->pathFilter;
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        static::assertSame(['a'], $rows->reduceToArray('value'));
    }

    public function test_a_body_column_predicate_is_pushed_as_a_filter_and_never_as_a_path_filter(): void
    {
        $extractor = PartitionedSourceMother::yearMonth()->withPruning();

        df()
            ->read($extractor)
            ->filter($predicate = ref('value')->equals(lit('a')))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->criteria[0]->pathFilter);
        static::assertSame([$predicate], $extractor->criteria[0]->conjuncts);
    }

    public function test_a_predicate_referencing_no_column_is_pushed_as_a_filter_and_never_as_a_path_filter(): void
    {
        $extractor = PartitionedSourceMother::yearMonth()->withPruning();

        df()
            ->read($extractor)
            ->filter($predicate = lit(1)->equals(lit(1)))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->criteria[0]->pathFilter);
        static::assertSame([$predicate], $extractor->criteria[0]->conjuncts);
    }

    public function test_a_mixed_predicate_pushes_both_conjuncts_and_only_the_partition_one_as_a_path_filter(): void
    {
        $extractor = PartitionedSourceMother::yearMonth()->withPruning();

        $rows = df()
            ->read($extractor)
            ->filter(($year = ref('year')->equals(lit(2023)))->and($value = ref('value')->notEquals(lit('a'))))
            ->fetch();

        $pathFilter = $extractor->criteria[0]->pathFilter;
        static::assertInstanceOf(Filters::class, $pathFilter);
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        static::assertEquals([$year, $value], $extractor->criteria[0]->conjuncts);
        // the read applies the body conjunct too: the only 2023 row has value "a"
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

        $pathFilter = $extractor->criteria[0]->pathFilter;
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

        $pathFilter = $extractor->criteria[0]->pathFilter;
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

        $pathFilter = $extractor->criteria[0]->pathFilter;
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2025/month=07')));
    }

    public function test_an_or_mixing_a_body_column_is_pushed_whole_as_a_filter_and_never_as_a_path_filter(): void
    {
        $extractor = PartitionedSourceMother::yearMonth()->withPruning();

        df()
            ->read($extractor)
            ->filter($predicate = ref('year')->equals(lit(2023))->or(ref('value')->equals(lit('x'))))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->criteria[0]->pathFilter);
        static::assertSame([$predicate], $extractor->criteria[0]->conjuncts);
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

        $pathFilter = $extractor->criteria[0]->pathFilter;
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

        static::assertEquals(new OnlyFiles(), $extractor->criteria[0]->pathFilter);
    }

    public function test_a_with_column_redefining_the_partition_column_blocks_the_push(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->withColumn('year', lit(2023))
            ->filter(ref('year')->equals(lit(2023)))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->criteria[0]->pathFilter);
    }

    public function test_a_duplicate_row_defining_the_partition_column_blocks_the_push(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        $rows = df()
            ->read($extractor)
            ->duplicateRow(lit(true), new WithColumn('year', lit(2023)))
            ->filter(ref('year')->equals(lit(2023)))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->criteria[0]->pathFilter);
        // the 2024 row's duplicate carries year=2023: pruning its partition would lose it
        static::assertCount(3, $rows);
    }

    public function test_a_duplicate_row_defining_another_column_does_not_block_the_push(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->duplicateRow(lit(true), new WithColumn('copy', lit(1)))
            ->filter(ref('year')->equals(lit(2023)))
            ->fetch();

        static::assertInstanceOf(Filters::class, $extractor->criteria[0]->pathFilter);
    }

    public function test_a_filter_a_write_root_does_not_consume_through_is_not_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->write(to_memory(new ArrayMemory()))
            ->filter(ref('year')->equals(lit(2023)))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->criteria[0]->pathFilter);
    }

    public function test_a_non_file_extractor_is_never_pushed_into(): void
    {
        $plan = NodeMother::plan(
            new Filter(new Read(new RecordingExtractor(schema(int_schema('year')))), ref('year')->equals(lit(2023))),
        );

        static::assertSame($plan, (new PushFilterIntoSource())->apply($plan, config()->backend()));
    }

    public function test_a_data_conjunct_is_pushed_into_a_source_without_partition_columns(): void
    {
        $extractor = (new RecordingFileExtractor(
            schema(int_schema('year')),
            array_to_rows([['year' => 2023]], schema(int_schema('year'))),
        ))->withPruning();

        df()
            ->read($extractor)
            ->filter($predicate = ref('year')->equals(lit(2023)))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->criteria[0]->pathFilter);
        static::assertSame([$predicate], $extractor->criteria[0]->conjuncts);
    }

    public function test_a_data_conjunct_the_source_does_not_prune_with_stays_in_its_filter_node(): void
    {
        $filter = (new PushFilterIntoSource())
            ->apply(
                NodeMother::plan(
                    new Filter(new Read(PartitionedSourceMother::yearMonth()), $value = ref('value')->isNotNull()),
                ),
                config()->backend(),
            )
            ->root->children()[0];

        static::assertInstanceOf(Filter::class, $filter);
        static::assertSame($value, $filter->function);
        $read = $filter->children()[0];
        static::assertInstanceOf(Read::class, $read);
        static::assertSame([], $read->criteria()->conjuncts);
        static::assertEquals(new OnlyFiles(), $read->criteria()->pathFilter);
    }

    public function test_only_the_partition_conjunct_is_pushed_into_a_source_that_does_not_prune(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        $rows = df()
            ->read($extractor)
            ->filter(($year = ref('year')->equals(lit(2023)))->and(ref('value')->notEquals(lit('a'))))
            ->fetch();

        $pathFilter = $extractor->criteria[0]->pathFilter;
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        static::assertEquals([$year], $extractor->criteria[0]->conjuncts);
        static::assertCount(0, $rows);
    }

    public function test_a_partition_conjunct_after_a_kept_one_prunes_the_listing_and_stays_in_its_node(): void
    {
        $filter = (new PushFilterIntoSource())
            ->apply(
                NodeMother::plan(
                    new Filter(
                        new Read(PartitionedSourceMother::yearMonth()),
                        ($value = ref('value')->isNotNull())->and($year = ref('year')->equals(lit(2023))),
                    ),
                ),
                config()->backend(),
            )
            ->root->children()[0];

        static::assertInstanceOf(Filter::class, $filter);
        static::assertEquals(new All($value, $year), $filter->function);
        $read = $filter->children()[0];
        static::assertInstanceOf(Read::class, $read);
        static::assertSame([], $read->criteria()->conjuncts);
        static::assertTrue($read->criteria()->pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($read->criteria()->pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
    }

    public function test_a_partition_filter_above_a_kept_conjunct_prunes_the_listing_and_stays_a_node(): void
    {
        $top = (new PushFilterIntoSource())
            ->apply(
                NodeMother::plan(
                    new Filter(
                        new Filter(new Read(PartitionedSourceMother::yearMonth()), $value = ref('value')->isNotNull()),
                        $year = ref('year')->equals(lit(2023)),
                    ),
                ),
                config()->backend(),
            )
            ->root->children()[0];

        static::assertInstanceOf(Filter::class, $top);
        static::assertSame($year, $top->function);
        $bottom = $top->children()[0];
        static::assertInstanceOf(Filter::class, $bottom);
        static::assertSame($value, $bottom->function);
        $read = $bottom->children()[0];
        static::assertInstanceOf(Read::class, $read);
        static::assertSame([], $read->criteria()->conjuncts);
        static::assertFalse($read->criteria()->pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
    }

    public function test_a_non_deterministic_data_conjunct_is_not_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->filter(ref('value')->equals(random_string(2)))
            ->fetch();

        static::assertSame([], $extractor->criteria[0]->conjuncts);
    }

    public function test_a_data_conjunct_under_a_reducing_node_is_not_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->limit(5)
            ->filter(ref('value')->equals(lit('a')))
            ->fetch();

        static::assertSame(5, $extractor->criteria[0]->limit);
        static::assertSame([], $extractor->criteria[0]->conjuncts);
    }

    public function test_a_non_deterministic_predicate_is_not_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->filter(ref('month')->equals(random_string(2)))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->criteria[0]->pathFilter);
    }

    public function test_a_predicate_calling_a_user_callable_is_not_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->filter(ref('year')->call(lit('is_string'), type_boolean()))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->criteria[0]->pathFilter);
    }

    public function test_a_constant_conjunct_is_pushed_and_stops_nothing_above_it(): void
    {
        $extractor = PartitionedSourceMother::yearMonth()->withPruning();

        $rows = df()
            ->read($extractor)
            ->filter($constant = lit(true))
            ->filter($year = ref('year')->equals(lit(2023)))
            ->fetch();

        static::assertSame([$constant, $year], $extractor->criteria[0]->conjuncts);
        static::assertSame([2023], $rows->reduceToArray('year'));
    }

    public function test_a_constant_false_conjunct_returns_no_row(): void
    {
        static::assertSame(0, df()->read(PartitionedSourceMother::yearMonth())->filter(lit(false))->fetch()->count());
    }

    public function test_a_filter_inside_a_joined_frame_is_pushed_with_that_frames_context(): void
    {
        $right = PartitionedSourceMother::yearMonth();

        df()
            ->read(from_array([['id' => 2023]]))
            ->join(df()->read($right)->filter(ref('year')->equals(lit(2023))), join_on(['id' => 'year'], 'r_'))
            ->fetch();

        $pathFilter = $right->criteria[0]->pathFilter;
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
        $frame = df()->read(PartitionedSourceMother::yearMonth())->filter(ref('year')->equals(lit(2023)));

        $frame->fetch();

        static::assertStringEndsWith(
            "Extractor: RecordingFileExtractor\n         Source: file:///dev/null",
            $frame->explain()->toString(Stage::unoptimized),
        );
        static::assertStringEndsWith(
            "Extractor: RecordingFileExtractor\n      Source: file:///dev/null\n      Files: year = 2023\n      Filter: year = 2023",
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

        $pathFilter = $extractor->criteria[0]->pathFilter;
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
            ->withColumn('y', ref('year'))
            ->filter(ref('y')->equals(lit(2023)))
            ->fetch();

        $pathFilter = $extractor->criteria[0]->pathFilter;
        static::assertInstanceOf(Filters::class, $pathFilter);
        static::assertTrue($pathFilter->accept(PartitionedSourceMother::file('year=2023/month=07')));
        static::assertFalse($pathFilter->accept(PartitionedSourceMother::file('year=2024/month=07')));
        static::assertSame([2023], $rows->reduceToArray('y'));
    }

    public function test_a_filter_on_an_alias_of_a_body_column_is_pushed_as_the_original_column_and_never_as_a_path_filter(): void
    {
        $extractor = PartitionedSourceMother::yearMonth()->withPruning();

        df()
            ->read($extractor)
            ->withColumn('v', ref('value'))
            ->filter(ref('v')->equals($literal = lit('a')))
            ->fetch();

        static::assertEquals(new OnlyFiles(), $extractor->criteria[0]->pathFilter);
        static::assertEquals([ref('value')->equals($literal)], $extractor->criteria[0]->conjuncts);
    }

    public function test_a_filter_node_whose_only_conjunct_is_pushed_is_removed(): void
    {
        $read = (new PushFilterIntoSource())
            ->apply(
                NodeMother::plan(
                    new Filter(new Read(PartitionedSourceMother::yearMonth()), $year = ref('year')->equals(lit(2023))),
                ),
                config()->backend(),
            )
            ->root->children()[0];

        static::assertInstanceOf(Read::class, $read);
        static::assertSame([$year], $read->criteria()->conjuncts);
    }

    public function test_a_pushed_conjunct_leaves_its_filter_node_and_the_rest_stays_in_it(): void
    {
        $filter = (new PushFilterIntoSource())
            ->apply(
                NodeMother::plan(
                    new Filter(
                        new Read(PartitionedSourceMother::yearMonth()),
                        ($year = ref('year')->equals(lit(2023)))->and($random = ref('value')->equals(random_string(2))),
                    ),
                ),
                config()->backend(),
            )
            ->root->children()[0];

        static::assertInstanceOf(Filter::class, $filter);
        static::assertSame($random, $filter->function);
        $read = $filter->children()[0];
        static::assertInstanceOf(Read::class, $read);
        static::assertSame([$year], $read->criteria()->conjuncts);
    }

    public function test_stacked_filter_nodes_are_pushed_leaf_first(): void
    {
        $read = (new PushFilterIntoSource())
            ->apply(
                NodeMother::plan(
                    new Filter(
                        new Filter(
                            new Read(PartitionedSourceMother::yearMonth()),
                            $year = ref('year')->equals(lit(2023)),
                        ),
                        $month = ref('month')->equals(lit('07')),
                    ),
                ),
                config()->backend(),
            )
            ->root->children()[0];

        static::assertInstanceOf(Read::class, $read);
        static::assertEquals([$year, $month], $read->criteria()->conjuncts);
    }

    public function test_a_filter_node_that_keeps_a_conjunct_stops_the_push_of_the_filters_above_it(): void
    {
        $top = (new PushFilterIntoSource())
            ->apply(
                NodeMother::plan(
                    new Filter(
                        new Filter(
                            new Read(PartitionedSourceMother::yearMonth()),
                            ($year = ref('year')->equals(lit(2023)))->and(
                                $random = ref('value')->equals(random_string(2)),
                            ),
                        ),
                        $month = ref('month')->equals(lit('07')),
                    ),
                ),
                config()->backend(),
            )
            ->root->children()[0];

        static::assertInstanceOf(Filter::class, $top);
        static::assertSame($month, $top->function);
        $bottom = $top->children()[0];
        static::assertInstanceOf(Filter::class, $bottom);
        static::assertSame($random, $bottom->function);
        $read = $bottom->children()[0];
        static::assertInstanceOf(Read::class, $read);
        static::assertSame([$year], $read->criteria()->conjuncts);
    }

    public function test_a_filter_node_keeping_two_conjuncts_keeps_them_as_one_all(): void
    {
        $filter = (new PushFilterIntoSource())
            ->apply(
                NodeMother::plan(
                    new Filter(
                        new Read(PartitionedSourceMother::yearMonth()),
                        ref('year')
                            ->equals(lit(2023))
                            ->and($first = ref('value')->equals(random_string(2)))
                            ->and($second = ref('value')->notEquals(random_string(3))),
                    ),
                ),
                config()->backend(),
            )
            ->root->children()[0];

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

        static::assertSame(1, $extractor->criteria[0]->limit);
        static::assertSame([$year], $extractor->criteria[0]->conjuncts);
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

        static::assertNull($extractor->criteria[0]->limit);
    }

    public function test_a_kept_conjunct_keeps_every_conjunct_after_it_in_its_node(): void
    {
        $filter = (new PushFilterIntoSource())
            ->apply(
                NodeMother::plan(
                    new Filter(
                        new Read(PartitionedSourceMother::yearMonth()),
                        ($year = ref('year')->equals(lit(2023)))->and(
                            $random = ref('value')->equals(random_string(2)),
                        )->and($month = ref('month')->equals(lit('07'))),
                    ),
                ),
                config()->backend(),
            )
            ->root->children()[0];

        static::assertInstanceOf(Filter::class, $filter);
        static::assertEquals(new All($random, $month), $filter->function);
        $read = $filter->children()[0];
        static::assertInstanceOf(Read::class, $read);
        static::assertSame([$year], $read->criteria()->conjuncts);
    }

    /**
     * @return iterable<string, array{Node}>
     */
    public static function nodes_removing_year(): iterable
    {
        $read = new Read(PartitionedSourceMother::yearMonth());

        yield 'rename' => [new Node\Rename($read, 'year', 'y')];
        yield 'drop' => [new Node\Drop($read, ['year'])];
        yield 'select' => [new Node\Select($read, ['month', 'value'])];
    }

    #[DataProvider('nodes_removing_year')]
    public function test_a_filter_on_a_column_a_node_removes_is_not_pushed_past_it(Node $node): void
    {
        $filter = (new PushFilterIntoSource())
            ->apply(NodeMother::plan(new Filter($node, $year = ref('year')->equals(lit(2023)))), config()->backend())
            ->root->children()[0];

        static::assertInstanceOf(Filter::class, $filter);
        static::assertSame($year, $filter->function);
    }

    public function test_a_filter_on_a_renamed_away_column_fails(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        df()
            ->read(PartitionedSourceMother::yearMonth())
            ->rename('year', 'y')
            ->filter(ref('year')->equals(lit(2023)))
            ->fetch();
    }

    public function test_a_filter_on_a_dropped_column_fails(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        df()
            ->read(PartitionedSourceMother::yearMonth())
            ->drop('year')
            ->filter(ref('year')->equals(lit(2023)))
            ->fetch();
    }

    public function test_a_filter_on_a_column_select_does_not_keep_fails(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        df()
            ->read(PartitionedSourceMother::yearMonth())
            ->select('month')
            ->filter(ref('year')->equals(lit(2023)))
            ->fetch();
    }

    public function test_a_filter_on_a_column_select_keeps_is_pushed(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();

        df()
            ->read($extractor)
            ->select('year')
            ->filter($year = ref('year')->equals(lit(2023)))
            ->fetch();

        static::assertSame([$year], $extractor->criteria[0]->conjuncts);
    }

    public function test_a_consumer_reading_through_the_filter_is_reached(): void
    {
        $filter = new Filter(NodeMother::read(), lit(true));

        static::assertTrue((new PushFilterIntoSource())->reachedByEveryConsumer(
            $filter,
            NodeMother::select($filter),
            $filter,
        ));
    }

    public function test_a_consumer_bypassing_the_filter_is_not_reached(): void
    {
        $read = NodeMother::read();
        $filter = new Filter($read, lit(true));

        static::assertFalse((new PushFilterIntoSource())->reachedByEveryConsumer(
            $filter,
            NodeMother::select($filter),
            NodeMother::limit($read, 5),
        ));
    }

    public function test_a_chain_that_never_reaches_the_filter_is_not_reached(): void
    {
        $filter = new Filter(NodeMother::read(), lit(true));

        static::assertFalse((new PushFilterIntoSource())->reachedByEveryConsumer(
            $filter,
            NodeMother::select(NodeMother::read()),
        ));
    }

    public function test_a_transparent_preserving_chain_reaches_the_leaf(): void
    {
        $read = NodeMother::read();

        static::assertNotNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(NodeMother::select($read, 'year'), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_another_filter_below_is_transparent(): void
    {
        $read = NodeMother::read();

        static::assertNotNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(new Filter($read, lit(true)), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_limit_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(NodeMother::limit($read, 5), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_an_offset_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(new Offset($read, 5), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_an_until_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(new Until($read, lit(true)), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_distinct_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(new Distinct($read, ['id']), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_discard_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(new Discard($read), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_an_opaque_node_below_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(NodeMother::sort($read), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_with_column_redefining_a_referenced_name_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(new Node\WithColumn($read, 'year', lit(1)), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_with_column_over_a_definition_redefining_a_referenced_name_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(new Node\WithColumn($read, int_schema('year'), lit(1)), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_with_column_over_another_name_does_not_block(): void
    {
        $read = NodeMother::read();

        static::assertNotNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(new Node\WithColumn($read, 'other', lit(1)), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_rename_to_a_referenced_name_rewrites_the_predicate_to_the_column_below(): void
    {
        $read = NodeMother::read();

        static::assertEquals(
            ref('id')->isNotNull(),
            (new PushFilterIntoSource())->predicateAtLeaf(
                new Filter(new Rename($read, 'id', 'year'), lit(true)),
                $read,
                ref('year')->isNotNull(),
            ),
        );
    }

    public function test_a_rename_to_another_name_does_not_block(): void
    {
        $read = NodeMother::read();

        static::assertNotNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(new Rename($read, 'id', 'other'), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_rename_each_always_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(new RenameEach($read, [rename_replace('_', '-')]), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_chain_that_does_not_end_in_the_leaf_does_not_reach(): void
    {
        static::assertNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(NodeMother::read(), lit(true)),
            NodeMother::read(),
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_duplicate_row_defining_a_referenced_name_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new PushFilterIntoSource())->predicateAtLeaf(new Filter(new DuplicateRow(
            $read,
            lit(true),
            [with_column('year', lit(2023))],
        ), lit(true)), $read, ref('year')->isNotNull()));
    }

    public function test_a_duplicate_row_defining_another_name_does_not_block(): void
    {
        $read = NodeMother::read();

        static::assertNotNull((new PushFilterIntoSource())->predicateAtLeaf(new Filter(new DuplicateRow(
            $read,
            lit(true),
            [with_column('copy', lit(1))],
        ), lit(true)), $read, ref('year')->isNotNull()));
    }

    public function test_a_node_redefining_a_referenced_column_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(new RedefiningNode($read, Redefined::names('year')), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_a_node_redefining_another_column_does_not_block(): void
    {
        $read = NodeMother::read();

        static::assertNotNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(new RedefiningNode($read, Redefined::names('other')), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_an_unknown_redefinition_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new PushFilterIntoSource())->predicateAtLeaf(
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
            (new PushFilterIntoSource())->predicateAtLeaf(
                new Filter(new Node\WithColumn($read, 'year', ref('id')), lit(true)),
                $read,
                ref('year')->isNotNull(),
            ),
        );
    }

    public function test_a_with_column_aliasing_a_column_under_a_definition_blocks(): void
    {
        $read = NodeMother::read();

        static::assertNull((new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(new Node\WithColumn($read, int_schema('year'), ref('id')), lit(true)),
            $read,
            ref('year')->isNotNull(),
        ));
    }

    public function test_stacked_aliases_rewrite_the_predicate_through_every_one(): void
    {
        $read = NodeMother::read();

        static::assertEquals(
            ref('id')->equals(ref('month')),
            (new PushFilterIntoSource())->predicateAtLeaf(
                new Filter(new Rename(new Node\WithColumn($read, 'copy', ref('id')), 'copy', 'year'), lit(true)),
                $read,
                ref('year')->equals(ref('month')),
            ),
        );
    }

    public function test_a_predicate_reading_no_redefined_name_arrives_unchanged(): void
    {
        $read = NodeMother::read();
        $predicate = ref('month')->isNotNull();

        static::assertSame($predicate, (new PushFilterIntoSource())->predicateAtLeaf(
            new Filter(new Rename($read, 'id', 'year'), lit(true)),
            $read,
            $predicate,
        ));
    }
}
