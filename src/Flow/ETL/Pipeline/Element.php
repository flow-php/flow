<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class Element
{
    private ?Transformer $transformer;

    private ?Loader $loader;

    private function __construct(?Transformer $transformer = null, ?Loader $loader = null)
    {
        $this->transformer = $transformer;
        $this->loader = $loader;
    }

    public static function transformer(Transformer $transformer) : self
    {
        return new self($transformer);
    }

    public static function loader(Loader $loader) : self
    {
        return new self(null, $loader);
    }

    public function process(Rows $rows) : Rows
    {
        if ($this->transformer) {
            return $this->transformer->transform($rows);
        }

        /**
         * @psalm-suppress PossiblyNullReference
         * @phpstan-ignore-next-line
         */
        $this->loader->load($rows);

        return $rows;
    }
}
