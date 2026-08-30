<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * Batch-level callback. Unlike CallbackRowTransformer the callable receives the whole Rows, so it
 * can read and declare the batch Schema - which a lazy DataFrame cannot supply up front.
 */
final class CallbackRowsTransformer implements Transformer
{
    /**
     * @var callable(Rows, FlowContext) : Rows
     */
    private $callable;

    /**
     * @param callable(Rows, FlowContext) : Rows $callable
     */
    public function __construct(callable $callable)
    {
        $this->callable = $callable;
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        return ($this->callable)($rows, $context);
    }
}
