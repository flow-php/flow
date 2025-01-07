<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;
use Flow\ETL\{FlowContext, Loader, Rows};

final readonly class BranchingLoader implements Closure, Loader, OverridingLoader
{
    public function __construct(
        private ScalarFunction $condition,
        private Loader $loader,
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
