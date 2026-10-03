<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use function assert;
use function is_array;

final readonly class ListPhysical implements Physical
{
    public function __construct(
        private Physical $element,
    ) {}

    public function toPhysical(mixed $value): mixed
    {
        assert(is_array($value));

        $physicals = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($value as $element) {
            $physicals[] = $element === null ? null : $this->element->toPhysical($element);
        }

        return $physicals;
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_array($physical));

        /** @var list<mixed> $physical */
        return $this->element->fromPhysicalAll($physical);
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            assert($physical === null || is_array($physical));

            /** @var null|list<mixed> $physical */
            $values[] = $physical === null ? null : $this->element->fromPhysicalAll($physical);
        }

        return $values;
    }
}
