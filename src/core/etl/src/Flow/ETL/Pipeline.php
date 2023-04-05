<?php

declare(strict_types=1);

namespace Flow\ETL;

/**
 * @internal
 */
interface Pipeline
{
    public function add(Loader|Transformer $pipe) : self;

    /**
     * Create clean instance of pipeline, with empty pipes and without source.
     */
    public function cleanCopy() : self;

    public function has(string $transformerClass) : bool;

    public function isAsync() : bool;

    /**
     * @return \Generator<Rows>
     */
    public function process(FlowContext $context) : \Generator;

    public function source(Extractor $extractor) : self;
}
