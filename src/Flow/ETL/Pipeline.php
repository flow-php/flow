<?php

declare(strict_types=1);

namespace Flow\ETL;

/**
 * @internal
 */
interface Pipeline
{
    public function add(Loader|Transformer $pipe) : void;

    /**
     * Create clean instance of pipeline, with empty pipes and without source.
     */
    public function cleanCopy() : self;

    public function onError(ErrorHandler $errorHandler) : void;

    /**
     * @return \Generator<int, Rows, mixed, void>
     */
    public function process(?int $limit = null) : \Generator;

    public function source(Extractor $extractor) : void;
}
