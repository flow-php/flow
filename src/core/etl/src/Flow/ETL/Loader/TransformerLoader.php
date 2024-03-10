<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\{FlowContext, Loader, Rows, Transformer};

final class TransformerLoader implements Loader, OverridingLoader
{
    public function __construct(
        private readonly Transformer $transformer,
        private readonly Loader $loader
    ) {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $this->loader->load($this->transformer->transform($rows, $context), $context);
    }

    public function loaders() : array
    {
        return [$this->loader];
    }
}
