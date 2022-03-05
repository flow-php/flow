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
    private Loader $loader;

    private Transformer $transformer;

    public function __construct(Transformer $transformer, Loader $loader)
    {
        $this->transformer = $transformer;
        $this->loader = $loader;
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
