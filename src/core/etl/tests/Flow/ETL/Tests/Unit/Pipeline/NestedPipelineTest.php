<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use function Flow\ETL\DSL\lit;
use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\NestedPipeline;
use Flow\ETL\Pipeline\ParallelizingPipeline;
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\EntryExpressionEvalTransformer;
use PHPUnit\Framework\TestCase;

final class NestedPipelineTest extends TestCase
{
    public function test_nested_pipelines() : void
    {
        $pipeline = new NestedPipeline(
            (new SynchronousPipeline())->add(new EntryExpressionEvalTransformer('active', lit(true))),
            new ParallelizingPipeline(new SynchronousPipeline(), 1)
        );

        $pipeline->setSource(new ProcessExtractor(
            new Rows(
                Row::create(Entry::integer('id', 1)),
                Row::create(Entry::integer('id', 2))
            )
        ));

        $this->assertEquals(
            [
                new Rows(
                    Row::create(Entry::integer('id', 1), Entry::boolean('active', true)),
                ),
                new Rows(
                    Row::create(Entry::integer('id', 2), Entry::boolean('active', true))
                ),
            ],
            \iterator_to_array($pipeline->process(new FlowContext(Config::default())))
        );
    }
}
