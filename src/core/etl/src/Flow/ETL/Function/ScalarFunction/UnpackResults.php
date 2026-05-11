<?php

declare(strict_types=1);

namespace Flow\ETL\Function\ScalarFunction;

use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row;

interface UnpackResults extends ScalarFunction
{
    /**
     * @return array<array-key, mixed>
     */
    public function eval(Row $row, FlowContext $context): array;
}
