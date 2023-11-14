<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Pipeline\Pipes;

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

    public function closure(FlowContext $context) : void;

    public function has(string $transformerClass) : bool;

    public function pipes() : Pipes;

    /**
     * @return \Generator<Rows>
     */
    public function process(FlowContext $context) : \Generator;

    public function setSource(Extractor $extractor) : self;

    public function source() : Extractor;
}
