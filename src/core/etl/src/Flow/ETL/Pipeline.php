<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Pipeline\{Pipes};

/**
 * @internal
 */
interface Pipeline
{
    public function add(Loader|Transformer $pipe) : self;

    public function has(string $transformerClass) : bool;

    public function pipes() : Pipes;

    /**
     * @return \Generator<int, Rows>
     */
    public function process(FlowContext $context) : \Generator;

    public function source() : Extractor;
}
