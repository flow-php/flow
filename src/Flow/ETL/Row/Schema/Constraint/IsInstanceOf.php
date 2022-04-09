<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Constraint;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema\Constraint;

/**
 * @implements Constraint<array{class: class-string}>
 */
final class IsInstanceOf implements Constraint
{
    /**
     * @var class-string
     */
    private string $class;

    /**
     * @param class-string $class
     */
    public function __construct(string $class)
    {
        $this->class = $class;
    }

    public function __serialize() : array
    {
        return [
            'class' => $this->class,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->class = $data['class'];
    }

    public function isSatisfiedBy(Entry $entry) : bool
    {
        return $entry->value() instanceof $this->class;
    }
}
