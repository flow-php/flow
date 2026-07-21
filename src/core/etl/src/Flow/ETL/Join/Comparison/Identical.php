<?php

declare(strict_types=1);

namespace Flow\ETL\Join\Comparison;

use Flow\ETL\Join\Comparison;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;

use function is_string;

final readonly class Identical implements Comparison
{
    public function __construct(
        private string|Reference $entryLeft,
        private string|Reference $entryRight,
    ) {}

    public function compare(Row $left, Row $right): bool
    {
        $leftValue = $left->valueOf($this->entryLeft);
        $rightValue = $right->valueOf($this->entryRight);

        // SQL semantics - null never equals anything, including null
        if ($leftValue === null || $rightValue === null) {
            return false;
        }

        return $leftValue === $rightValue;
    }

    /**
     * @return array<Reference>
     */
    public function left(): array
    {
        return [is_string($this->entryLeft) ? EntryReference::init($this->entryLeft) : $this->entryLeft];
    }

    /**
     * @return array<Reference>
     */
    public function right(): array
    {
        return [is_string($this->entryRight) ? EntryReference::init($this->entryRight) : $this->entryRight];
    }
}
