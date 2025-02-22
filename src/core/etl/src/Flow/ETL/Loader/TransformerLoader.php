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
            $rows = $this->transformer->transform($rows, $context);
        } else {
            $rows = df()->from(from_rows($rows))->with($this->transformer)->fetch();
        }

        $this->loader->load($rows, $context);
    }

    public function loaders() : array
    {
        return [$this->loader];
    }
}
