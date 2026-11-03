<?php

declare(strict_types=1);

namespace Flow\ETL\API\Pipe;

use Flow\ETL\API\DataFrame;
use Flow\ETL\Values\Schema;

final readonly class GetSchema
{
    public function __invoke(DataFrame $dataFrame): Schema
    {
        return $dataFrame->schema();
    }
}
