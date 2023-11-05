<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Extractor\PipelineExtractor;
use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class PipelineExtractorTest extends TestCase
{
    public function test_pipeline_extractor() : void
    {
        $pipeline = new SynchronousPipeline();
        $pipeline->setSource(new ProcessExtractor(
            new Rows(Row::create(Entry::integer('id', 1)), Row::create(Entry::integer('id', 2))),
            new Rows(Row::create(Entry::integer('id', 3)), Row::create(Entry::integer('id', 4))),
            new Rows(Row::create(Entry::integer('id', 5)), Row::create(Entry::integer('id', 6))),
        ));

        $extractor = new PipelineExtractor($pipeline, Config::default());

        $this->assertCount(
            3,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }
}
