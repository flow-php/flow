<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Sink\Transformed;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\SpyTransformer;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;

use function array_column;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\limit;

final class PerRunPlanTest extends FlowIntegrationTestCase
{
    public function test_a_limit_fetched_twice_yields_the_limit_twice(): void
    {
        $dataFrame = df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->limit(3);

        static::assertCount(3, $dataFrame->fetch());
        static::assertCount(3, $dataFrame->fetch());
    }

    public function test_a_stateful_transformer_starts_from_its_constructed_state_on_every_run(): void
    {
        $dataFrame = df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->transform(new AddRowIndexTransformer('idx', StartFrom::ZERO));

        static::assertSame([0, 1, 2], array_column($dataFrame->fetch()->toArray(), 'idx'));
        static::assertSame([0, 1, 2], array_column($dataFrame->fetch()->toArray(), 'idx'));
    }

    public function test_the_users_own_transformer_keeps_its_state_across_runs(): void
    {
        $transformer = new SpyTransformer();
        $dataFrame = df()->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))->transform($transformer);

        $dataFrame->run();
        $dataFrame->run();

        static::assertSame(6, $transformer->seen);
    }

    public function test_a_side_root_that_hit_its_limit_hits_its_own_limit_on_the_next_run(): void
    {
        $loader = new SpyLoader();
        $dataFrame = df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->write(new Transformed(limit(1), $loader));

        $dataFrame->run();
        $dataFrame->run();

        static::assertSame([1, 1], $loader->loadedRowCounts());
    }
}
