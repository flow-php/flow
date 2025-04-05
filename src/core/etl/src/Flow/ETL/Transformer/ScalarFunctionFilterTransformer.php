<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext, Row, Rows, Transformer};
use Flow\ETL\Function\ScalarFunction;

final readonly class ScalarFunctionFilterTransformer implements Transformer
{
    public function __construct(
        public ScalarFunction $function,
    ) {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->filter(function (Row $r) : bool {
            $value = $this->function->eval($r);

            if ($value instanceof ScalarFunction\ScalarResult) {
                $value = $value->value;
            }

            return (bool) $value;
        });
    }
}
