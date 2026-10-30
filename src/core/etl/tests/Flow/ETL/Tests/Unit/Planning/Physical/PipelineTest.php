<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Physical;

use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Physical\Pipeline;
use Flow\ETL\Planning\Physical\PipelineSteps;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\from_array;

final class PipelineTest extends FlowTestCase
{
    public function test_a_pipeline_reading_a_source_has_no_input_pipeline(): void
    {
        $read = new Read(from_array([['id' => 1]]));
        $steps = new PipelineSteps();
        $pipeline = new Pipeline(3, $read, $steps);

        static::assertSame(3, $pipeline->id);
        static::assertSame($read, $pipeline->input);
        static::assertSame($steps, $pipeline->steps);
        static::assertNull($pipeline->input());
        static::assertSame([$pipeline], $pipeline->chain());
    }

    public function test_a_fed_pipeline_has_no_input(): void
    {
        $pipeline = new Pipeline(0, null, new PipelineSteps());

        static::assertNull($pipeline->input);
        static::assertNull($pipeline->input());
        static::assertSame([$pipeline], $pipeline->chain());
    }

    public function test_a_pipeline_reading_another_has_it_as_input_and_chains_leaf_first(): void
    {
        $leaf = new Pipeline(0, new Read(from_array([['id' => 1]])), new PipelineSteps());
        $middle = new Pipeline(1, $leaf, new PipelineSteps());
        $root = new Pipeline(2, $middle, new PipelineSteps());

        static::assertSame($middle, $root->input());
        static::assertSame([$leaf, $middle, $root], $root->chain());
    }
}
