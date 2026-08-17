<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\DataFrame;
use Flow\ETL\Transformation;

final readonly class CallbackTransformation implements Transformation
{
    /**
     * @param callable(DataFrame): DataFrame $callback
     */
    public function __construct(
        private mixed $callback,
    ) {}

    public function transform(DataFrame $dataFrame): DataFrame
    {
        return ($this->callback)($dataFrame);
    }
}
