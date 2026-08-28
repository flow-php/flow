<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Row\Reference;
use Flow\ETL\Row\UnresolvedReference;

trait EntryRef
{
    private ?Reference $ref = null;

    abstract public function name(): string;

    public function ref(): Reference
    {
        if ($this->ref === null) {
            $this->ref = new UnresolvedReference($this->name());
        }

        return $this->ref;
    }
}
