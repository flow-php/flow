<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;

trait EntryRef
{
    private ?Reference $ref = null;

    abstract public function name() : string;

    public function ref() : Reference
    {
        if ($this->ref === null) {
            $this->ref = new EntryReference($this->name());
        }

        return $this->ref;
    }
}
