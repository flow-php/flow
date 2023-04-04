<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{expression: Expression}>
 */
final class EntryExpressionFilterTransformer implements Transformer
{
    public function __construct(
        private readonly Expression $expression
    ) {
    }

    public function __serialize() : array
    {
        return [
            'expression' => $this->expression,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->expression = $data['expression'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->filter(fn (Row $r) : bool => (bool) $this->expression->eval($r));
    }
}
