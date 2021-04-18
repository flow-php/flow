<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Pipeline\Element;

final class ETL
{
    private Extractor $extractor;

    private Pipeline $pipeline;

    private function __construct(Extractor $extractor, Pipeline $pipeline)
    {
        $this->extractor = $extractor;
        $this->pipeline = $pipeline;
    }

    public static function extract(Extractor $extractor, Pipeline $pipeline = null) : self
    {
        return new self($extractor, $pipeline ?? new SynchronousPipeline());
    }

    public function transform(Transformer $transformer) : self
    {
        $this->pipeline->register(Element::transformer($transformer));

        return $this;
    }

    public function load(Loader $loader) : self
    {
        $this->pipeline->register(Element::loader($loader));

        return $this;
    }

    public function run() : void
    {
        $this->pipeline->process($this->extractor->extract());
    }
}
