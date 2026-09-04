<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Inference;

final readonly class SchemaInference
{
    /**
     * @param int<1, max>|-1 $sampleSize - rows observed across every sampled source before the schema is frozen; -1 observes all
     * @param int<1, max>|-1 $filesToSniff - sources opened while the row budget is not spent; -1 opens all.
     * @param null|InferredTypes $types - the types inference may produce; null is every type but markup
     * @param bool $unionByName - false takes the column set from the first source and treats a later source's
     *                            unseen column as a divergence; true unions the sets and runs no divergence check
     */
    public function __construct(
        public int $sampleSize = 20_480,
        public int $filesToSniff = 10,
        public ?InferredTypes $types = null,
        public bool $unionByName = false,
    ) {}

    public function candidates(): InferredTypes
    {
        return $this->types ?? InferredTypes::default();
    }
}
