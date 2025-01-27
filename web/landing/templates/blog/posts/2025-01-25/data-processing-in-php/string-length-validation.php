<?php

use Flow\ETL\DataFrame;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Transformation;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\when;

final readonly class StringLengthValidation implements Transformation
{
    public function __construct(
        private EntryReference $reference,
        private int $min,
        private int $max,
        private string $outputEntry = 'valid'
    ) {
    }

    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame
            ->withEntry(
                $this->outputEntry,
                when(
                    ref($this->reference)->isType(type_string())
                        ->and(ref($this->reference)->size()->between($this->min, $this->max)),
                    ref($this->outputEntry),
                    lit(false)
                )
            )
        ;
    }
}