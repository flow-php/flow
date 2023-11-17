<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{function: ScalarFunction}>
 */
final class ScalarFunctionFilterTransformer implements Transformer
{
    public function __construct(
        private readonly ScalarFunction $function
    ) {
    }

    public function __serialize() : array
    {
        return [
            'function' => $this->function,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->function = $data['function'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->filter(fn (Row $r) : bool => (bool) $this->function->eval($r));
    }
}
