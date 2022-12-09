<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\StructureReference;

function col(string $entry, string ...$entries) : Reference
{
    if (\count($entries)) {
        return new StructureReference($entry, ...$entries);
    }

    return new EntryReference($entry);
}

function entry(string $entry) : EntryReference
{
    return new EntryReference($entry);
}

function struct(string ...$entries) : StructureReference
{
    if (!\count($entries)) {
        throw new InvalidArgumentException('struct (StructureReference) require at least one entry');
    }

    $entry = \array_shift($entries);

    return new StructureReference($entry, ...$entries);
}
