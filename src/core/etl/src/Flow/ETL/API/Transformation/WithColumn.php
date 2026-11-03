<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\WindowFunction;
use Flow\ETL\Values\Schema\Definition;
use NoDiscard;

final readonly class WithColumn implements Transformation
{
    /**
     * @param Definition<mixed>|string $name
     */
    public function __construct(
        public string|Definition $name,
        public ScalarFunction|WindowFunction $function,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->withColumn($this->name, $this->function);
    }
}
