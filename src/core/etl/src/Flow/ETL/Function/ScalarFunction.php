<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\{FlowContext, Row};

interface ScalarFunction
{
    public function eval(Row $row, FlowContext $context) : mixed;
}
