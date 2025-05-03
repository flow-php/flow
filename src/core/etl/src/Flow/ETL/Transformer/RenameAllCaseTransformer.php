<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext, Rows, Transformer, Transformer\StyleConverter\RenameStrategy};

/**
 * @deprecated use RenameEachTransformer with a selected RenameStrategy
 */
final class RenameAllCaseTransformer implements Transformer
{
    private RenameEachTransformer $transformer;

    public function __construct(
        bool $upper = false,
        bool $lower = false,
        bool $ucfirst = false,
        bool $ucwords = false,
    ) {
        if ($upper) {
            $this->transformer = new RenameEachTransformer(RenameStrategy::UPPER);
        }

        if ($lower) {
            $this->transformer = new RenameEachTransformer(RenameStrategy::LOWER);
        }

        if ($ucfirst) {
            $this->transformer = new RenameEachTransformer(RenameStrategy::UCFIRST);
        }

        if ($ucwords) {
            $this->transformer = new RenameEachTransformer(RenameStrategy::UCWORDS);
        }
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $this->transformer->transform($rows, $context);
    }
}
