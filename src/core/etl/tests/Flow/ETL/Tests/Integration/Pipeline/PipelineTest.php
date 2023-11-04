<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use Flow\ETL\GroupBy;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\CollectingPipeline;
use Flow\ETL\Pipeline\GroupByPipeline;
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\ETL\Transformer;
use PHPUnit\Framework\TestCase;

final class PipelineTest extends TestCase
{
    public function test_getting_pipes_from_nested_pipelines() : void
    {
        $synchronous = new SynchronousPipeline();
        $synchronous->add($transformer1 = $this->createMock(Transformer::class));
        $synchronous->add($transformer2 = $this->createMock(Transformer::class));
        $limiting = new GroupByPipeline(new GroupBy(), $synchronous);
        $limiting->add($transformer3 = $this->createMock(Transformer::class));
        $limiting->add($loader1 = $this->createMock(Loader::class));
        $limiting->add($transformer4 = $this->createMock(Transformer::class));
        $collecting = new CollectingPipeline($limiting);
        $collecting->add($loader2 = $this->createMock(Loader::class));

        $this->assertSame(
            [
                $transformer1,
                $transformer2,
                $transformer3,
                $loader1,
                $transformer4,
                $loader2,
            ],
            $collecting->pipes()->all()
        );

    }
}
