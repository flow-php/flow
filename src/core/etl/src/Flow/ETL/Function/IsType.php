<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;

final class IsType implements ScalarFunction
{
    /**
     * @var array<class-string<Entry>>
     */
    private array $typeClasses;

    /**
     * @param class-string<Entry> ...$typeClasses
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly ScalarFunction $ref,
        string ...$typeClasses
    ) {
        foreach ($typeClasses as $typeClass) {
            if (!\class_exists($typeClass) || !\in_array(Entry::class, \class_implements($typeClass), true)) {
                throw new InvalidArgumentException('"' . $typeClass . '" is not valid Entry Type class');
            }
        }

        $this->typeClasses = $typeClasses;
    }

    public function eval(Row $row) : bool
    {
        if (!$this->ref instanceof EntryReference) {
            return false;
        }

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
