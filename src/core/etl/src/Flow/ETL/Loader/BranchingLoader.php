<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use function Flow\ETL\DSL\{df, from_rows};
use Flow\ETL\{FlowContext, Loader, Rows, Transformation};
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;

final class BranchingLoader implements Closure, Loader, OverridingLoader
{
    private ?Transformation $transformation = null;

    public function __construct(
        private readonly ScalarFunction $condition,
        private readonly Loader $loader,
    ) {
    }

    #[\Override]
    public function closure(FlowContext $context) : void
    {
        if ($this->loader instanceof Closure) {
            $this->loader->closure($context);
        }
    }

    #[\Override]
    public function load(Rows $rows, FlowContext $context) : void
    {
        $rows = (new ScalarFunctionFilterTransformer($this->condition))->transform($rows, $context);

        if ($this->transformation) {
            $rows = df($context->config)
                ->read(from_rows($rows))
                ->with($this->transformation)
                ->fetch();
        }

        $this->loader->load(
            $rows,
            $context
        );
    }

    #[\Override]
    public function loaders() : array
    {
        return [
            $this->loader,
        ];
    }

    public function withTransformation(Transformation $transformation) : self
    {
        $this->transformation = $transformation;

        return $this;
    }
}
