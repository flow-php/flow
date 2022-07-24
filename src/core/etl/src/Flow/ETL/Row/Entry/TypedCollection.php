<?php declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Row\Entry\TypedCollection\Type;

interface TypedCollection
{
    public function type() : Type;
}
