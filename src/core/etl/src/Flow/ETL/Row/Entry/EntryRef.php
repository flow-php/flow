<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Row\EntryReference;

trait EntryRef
{
    private ?EntryReference $ref = null;

    abstract public function name() : string;

    public function ref() : EntryReference
    {
        if ($this->ref === null) {
            $this->ref = new EntryReference($this->name());
        }

        return $this->ref;
    }
}
