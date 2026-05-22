<?php

declare(strict_types=1);

namespace Flow\ETL\Join\Comparison;

use DateInterval;
use DateTimeInterface;
use Flow\ETL\Join\Comparison;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;

use function is_array;
use function is_bool;
use function is_numeric;
use function is_string;

final readonly class Equal implements Comparison
{
    public function __construct(
        private string|Reference $entryLeft,
        private string|Reference $entryRight,
    ) {}

    public function compare(Row $left, Row $right): bool
    {
        $leftValue = $left->valueOf($this->entryLeft);
        $rightValue = $right->valueOf($this->entryRight);

        if ($leftValue === null || $rightValue === null) {
            return $leftValue === $rightValue;
        }

        if (is_numeric($leftValue) && is_numeric($rightValue)) {
            return (float) $leftValue == (float) $rightValue;
        }

        if (is_string($leftValue) && is_string($rightValue)) {
            return $leftValue === $rightValue;
        }

        if (is_bool($leftValue) && is_bool($rightValue)) {
            return $leftValue === $rightValue;
        }

        if ($leftValue instanceof DateTimeInterface && $rightValue instanceof DateTimeInterface) {
            return $leftValue == $rightValue;
        }

        if ($leftValue instanceof DateInterval && $rightValue instanceof DateInterval) {
            return $leftValue == $rightValue;
        }

        if (is_array($leftValue) && is_array($rightValue)) {
            return $leftValue === $rightValue;
        }

        return false;
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
