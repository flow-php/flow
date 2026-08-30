<?php

declare(strict_types=1);

namespace Flow\ETL\Join\Comparison;

use Flow\ETL\Join\Comparison;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\UnresolvedReference;
use Flow\Types\Value\Uuid;

use function is_array;
use function is_bool;
use function is_numeric;
use function is_object;
use function is_string;

final readonly class Equal implements Comparison
{
    public function __construct(
        private string|Reference $entryLeft,
        private string|Reference $entryRight,
    ) {}

    public function compare(Row $left, Row $right): bool
    {
        $leftValue = $left->get($this->entryLeft);
        $rightValue = $right->get($this->entryRight);

        // SQL semantics - null never equals anything, including null
        if ($leftValue === null || $rightValue === null) {
            return false;
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

        if (is_array($leftValue) && is_array($rightValue)) {
            return $leftValue === $rightValue;
        }

        if (is_object($leftValue) && is_object($rightValue)) {
            if ($leftValue instanceof Uuid && $rightValue instanceof Uuid) {
                return $leftValue->isEqual($rightValue);
            }

            return $leftValue == $rightValue;
        }

        return false;
    }

    /**
     * @return array<Reference>
     */
    public function left(): array
    {
        return [is_string($this->entryLeft) ? UnresolvedReference::init($this->entryLeft) : $this->entryLeft];
    }

    /**
     * @return array<Reference>
     */
    public function right(): array
    {
        return [is_string($this->entryRight) ? UnresolvedReference::init($this->entryRight) : $this->entryRight];
    }
}
