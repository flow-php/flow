<?php

declare(strict_types=1);

namespace Flow\ETL;

final class ETL
{
    private Extractor $extractor;

    private Transformers $transformers;

    private function __construct(Extractor $extractor, Transformers $transformers)
    {
        $this->extractor = $extractor;
        $this->transformers = $transformers;
    }

    public static function extract(Extractor $extractor) : self
    {
        return new self($extractor, new Transformers());
    }

    public function transform(Transformer ...$transformer) : self
    {
        return new self($this->extractor, $this->transformers->add(...$transformer));
    }

    public function load(Loader ...$loaders) : void
    {
        foreach ($this->extractor->extract() as $rows) {
            $transformedRows = $this->transformers->transform($rows);

            foreach ($loaders as $loader) {
                $loader->load($transformedRows);
            }
        }
    }
}
