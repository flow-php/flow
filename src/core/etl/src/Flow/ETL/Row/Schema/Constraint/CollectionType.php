<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Constraint;

use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema\Constraint;

/**
 * @implements Constraint<array{type: Type}>
 */
final class CollectionType implements Constraint
{
    public function __construct(private readonly Type $type)
    {
    }

    public function __serialize() : array
    {
        return ['type' => $this->type];
    }

    public function __unserialize(array $data) : void
    {
        $this->type = $data['type'];
    }

    public function isSatisfiedBy(Entry $entry) : bool
    {
        if (!$entry instanceof Entry\TypedCollection) {
            return false;
        }

        return $entry->type()->element()->isEqual($this->type);
    }
}
