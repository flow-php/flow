<?php

declare(strict_types=1);

namespace Flow\ETL\Join\Comparison;

use Flow\ETL\Column\ComparableValues;
use Flow\ETL\Join\Comparison;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\UnresolvedReference;
use Flow\ETL\Rows;

use function Flow\Types\DSL\type_bare;
use function is_string;

final readonly class Identical implements Comparison
{
    public function __construct(
        private string|Reference $entryLeft,
        private string|Reference $entryRight,
    ) {}

    public function compare(Rows $left, Rows $right): array
    {
        $leftColumn = $left->column($this->left()[0]->base());
        $rightColumn = $right->column($this->right()[0]->base());
        $comparable = new ComparableValues();
        $sameType = type_bare($leftColumn->type())::class === type_bare($rightColumn->type())::class;
        $rightValues = $sameType ? $comparable->equality($rightColumn) : $rightColumn->values();
        $result = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($sameType ? $comparable->equality($leftColumn) : $leftColumn->values() as $i => $leftValue) {
            // SQL semantics - null never equals anything, including null
            $result[] = $leftValue !== null && $rightValues[$i] !== null && $leftValue === $rightValues[$i];
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
