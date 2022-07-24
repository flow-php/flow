<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Constraint;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema\Constraint;

/**
 * @implements Constraint<array{value: mixed}>
 */
final class SameAs implements Constraint
{
    public function __construct(private readonly mixed $value)
    {
    }

    public function __serialize() : array
    {
        return [
            'value' => $this->value,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->value = $data['value'];
    }

    public function isSatisfiedBy(Entry $entry) : bool
    {
        return $entry->value() === $this->value;
    }
}
