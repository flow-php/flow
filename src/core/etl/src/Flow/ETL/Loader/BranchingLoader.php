<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;

final class BranchingLoader implements Closure, Loader, OverridingLoader
{
    public function __construct(
        private readonly ScalarFunction $condition,
        private readonly Loader $loader
    ) {
    }

    public function closure(FlowContext $context) : void
    {
        if ($this->loader instanceof Closure) {
            $this->loader->closure($context);
        }
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $this->loader->load(
            (new ScalarFunctionFilterTransformer($this->condition))->transform($rows, $context),
            $context
        );
    }

    public function loaders() : array
    {
        return [
            $this->loader,
        ];
    }
}
