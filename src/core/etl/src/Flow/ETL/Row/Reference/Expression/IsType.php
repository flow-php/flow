<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference\Expression;

final class IsType implements Expression
{
    /**
     * @var array<string>
     */
    private array $typeClasses;

    public function __construct(
        private readonly EntryReference $ref,
        string ...$typeClasses
    ) {
        foreach ($typeClasses as $typeClass) {
            if (!\class_exists($typeClass) || !\in_array(Row\Entry::class, \class_implements($typeClass), true)) {
                throw new InvalidArgumentException('"' . $typeClass . '" is not valid Entry Type class');
            }
        }

        $this->typeClasses = $typeClasses;
    }

    public function eval(Row $row) : bool
    {
        if (!$row->has($this->ref)) {
            return false;
        }

        $entry = $row->get($this->ref);

        foreach ($this->typeClasses as $typeClass) {
            if (\is_a($entry, $typeClass, true)) {
                return true;
            }
        }

        return false;
    }
}
