<?php declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\PHP\Type\Logical\ListType;

interface TypedCollection
{
    public function type() : ListType;
}
