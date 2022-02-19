<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class TransformerLoader implements Loader
{
    private Transformer $transformer;

    private Loader $loader;

    public function __construct(Transformer $transformer, Loader $loader)
    {
        $this->transformer = $transformer;
        $this->loader = $loader;
    }

    /**
     * @return array{transformer: Transformer, loader: Loader}
     */
    public function __serialize() : array
    {
        return [
            'transformer' => $this->transformer,
            'loader' => $this->loader,
        ];
    }

    /**
     * @param array{transformer: Transformer, loader: Loader} $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
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
