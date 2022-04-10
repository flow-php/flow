<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Loader<array{transformer: Transformer, loader: Loader}>
 */
final class TransformerLoader implements Loader
{
    public function __construct(
        private readonly Transformer $transformer,
        private readonly Loader $loader
    ) {
    }

    public function __serialize() : array
    {
        return [
            'transformer' => $this->transformer,
            'loader' => $this->loader,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->transformer = $data['transformer'];
        $this->loader = $data['loader'];
    }

    public function load(Rows $rows) : void
    {
        $this->loader->load($this->transformer->transform($rows));
    }
}
