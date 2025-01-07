<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\{FlowContext, Row, Rows, Transformer};

final readonly class ScalarFunctionFilterTransformer implements Transformer
{
    public function __construct(
        public ScalarFunction $function,
    ) {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->filter(fn (Row $r) : bool => (bool) $this->function->eval($r));
    }
}
