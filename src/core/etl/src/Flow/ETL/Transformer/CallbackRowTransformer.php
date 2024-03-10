<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext, Row, Rows, Transformer};

final class CallbackRowTransformer implements Transformer
{
    /**
     * @phpstan-var callable(Row) : Row
     */
    private $callable;

    /**
     * @param callable(Row) : Row $callable
     */
    public function __construct(callable $callable)
    {
        $this->callable = $callable;
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map($this->callable);
    }
}
