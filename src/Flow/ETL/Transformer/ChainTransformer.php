<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @psalm-immutable
 */
final class ChainTransformer implements Transformer
{
    /**
     * @var Transformer[]
     */
    private array $transformers;

    public function __construct(Transformer ...$transformers)
    {
        $this->transformers = $transformers;
    }

    /**
     * @return array{transformers: array<Transformer>}
     */
    public function __serialize() : array
    {
        return [
            'transformers' => $this->transformers,
        ];
    }

    /**
     * @param array{transformers: array<Transformer>} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->transformers = $data['transformers'];
    }

    public function transform(Rows $rows) : Rows
    {
        foreach ($this->transformers as $transformer) {
            $rows = $transformer->transform($rows);
        }

        return $rows;
    }
}
