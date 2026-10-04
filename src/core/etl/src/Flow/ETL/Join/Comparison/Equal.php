<?php

declare(strict_types=1);

namespace Flow\ETL\Join\Comparison;

use Flow\ETL\Join\Comparison;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\UnresolvedReference;
use Flow\ETL\Rows;
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

    public function compare(Rows $left, Rows $right): array
    {
        $rightValues = $right->column($this->right()[0]->base())->values();
        $result = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($left->column($this->left()[0]->base())->values() as $i => $leftValue) {
            // @mago-ignore analysis:mixed-assignment
            $rightValue = $rightValues[$i];

            $result[] = match (true) {
                // SQL semantics - null never equals anything, including null
                $leftValue === null || $rightValue === null => false,
                is_numeric($leftValue) && is_numeric($rightValue) => (float) $leftValue == (float) $rightValue,
                is_string($leftValue) && is_string($rightValue) => $leftValue === $rightValue,
                is_bool($leftValue) && is_bool($rightValue) => $leftValue === $rightValue,
                is_array($leftValue) && is_array($rightValue) => $leftValue === $rightValue,
                $leftValue instanceof Uuid && $rightValue instanceof Uuid => $leftValue->isEqual($rightValue),
                is_object($leftValue) && is_object($rightValue) => $leftValue == $rightValue,
                default => false,
            };
        }

        return $result;
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
