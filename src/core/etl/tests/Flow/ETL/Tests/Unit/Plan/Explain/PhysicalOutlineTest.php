<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Explain;

use Flow\ETL\Plan\Explain\PhysicalOutline;
use Flow\ETL\Plan\Explain\TreeLayout;
use Flow\ETL\Tests\Double\UndescribableRowLessExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\PhysicalPlanMother;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\to_output;

final class PhysicalOutlineTest extends FlowTestCase
{
    public function test_a_plan_without_a_blocking_step_is_one_pipeline(): void
    {
        $root = (new PhysicalOutline())->of(PhysicalPlanMother::reading(from_array([['id' => 1]])));

        static::assertSame('Physical plan', $root->name);
        static::assertSame(['Columns: id'], $root->lines);
        static::assertCount(1, $root->children);
        static::assertSame('Pipeline #0', $root->children[0]->name);
        static::assertSame(['Extractor: ArrayExtractor'], $root->children[0]->lines);
        static::assertSame([], $root->children[0]->children);
    }

    public function test_every_step_is_listed_in_the_order_the_rows_reach_it(): void
    {
        static::assertSame(
            <<<'PLAN'
                Physical plan
                │  Columns: id
                └─ Pipeline #0
                      Extractor: ArrayExtractor
                      Processor: CollectingProcessor
                         Schema: declared
                      Loader: StreamLoader
                PLAN,
            (new TreeLayout())->render((new PhysicalOutline())->of(PhysicalPlanMother::of(
                data_frame()
                    ->read(from_array([['id' => 1]]))
                    ->collect()
                    ->write(to_output(truncate: false)),
            ))),
        );
    }

    public function test_a_joined_frame_is_a_plan_of_its_own_under_the_step_that_reads_it(): void
    {
        static::assertSame(
            <<<'PLAN'
                Physical plan
                │  Columns: id, joined_id
                └─ Pipeline #1
                   │  Processor: CollectingProcessor
                   │     Schema: declared
                   │  Loader: StreamLoader
                   └─ Pipeline #0
                      │  Extractor: ArrayExtractor
                      │  Processor: HashJoinProcessor
                      │     Join: left
                      │     On: id = id
                      │     Prefix: joined_
                      │     Storage: FilesystemBuckets
                      │     Buckets: 64
                      │     Batch: 1000
                      └─ Right side: Pipeline #0
                            Extractor: ArrayExtractor
                PLAN,
            (new TreeLayout())->render((new PhysicalOutline())->of(PhysicalPlanMother::of(
                data_frame()
                    ->read(from_array([['id' => 1]]))
                    ->join(
                        data_frame()->read(from_array([['id' => 1]])),
                        join_on(['id' => 'id'], join_prefix: 'joined_'),
                    )
                    ->collect()
                    ->write(to_output(truncate: false)),
            ))),
        );
    }

    public function test_a_pushed_limit_is_listed_under_the_source_that_was_handed_it(): void
    {
        $root = (new PhysicalOutline())->of(PhysicalPlanMother::of(
            data_frame()->read(from_array([['id' => 1], ['id' => 2]]))->limit(1),
        ));

        static::assertSame(
            ['Extractor: ArrayExtractor', '   Limit: 1', 'Transformer: LimitTransformer'],
            $root->children[0]->lines,
        );
    }

    public function test_a_plan_that_could_not_derive_its_schema_says_why(): void
    {
        $root = (new PhysicalOutline())->of(PhysicalPlanMother::reading(new UndescribableRowLessExtractor()));

        static::assertStringStartsWith('Schema: not derivable - ', $root->lines[0]);
    }
}
