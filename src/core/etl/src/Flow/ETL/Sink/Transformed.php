<?php

declare(strict_types=1);

namespace Flow\ETL\Sink;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Loader;
use Flow\ETL\Sink;
use Flow\ETL\Transformation;
use Flow\ETL\Transformer;

final readonly class Transformed implements Sink
{
    public function __construct(
        private Transformer|Transformation $transformer,
        private Loader|Sink $sink,
    ) {}

    public function write(DataFrame $prefix): void
    {
        if ($prefix->with($this->transformer) !== $prefix) {
            throw InvalidLogicException::transformationReturnedAnotherFrame($this->transformer::class);
        }

        $prefix->write($this->sink);
    }
}
