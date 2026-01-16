<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use function Flow\ETL\DSL\{df, from_rows};
use Flow\ETL\{FlowContext, Loader, Rows, Transformation, Transformer};

final readonly class TransformerLoader implements Closure, Loader, OverridingLoader
{
    public function __construct(
        private Transformer|Transformation $transformer,
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
        if ($this->transformer instanceof Transformer) {
            $this->loader->load($this->transformer->transform($rows, $context), $context);
        } else {
            df($context->config)->from(from_rows($rows))->with($this->transformer)->load($this->loader)->run();
        }
    }

    public function loaders() : array
    {
        return [$this->loader];
    }
}
