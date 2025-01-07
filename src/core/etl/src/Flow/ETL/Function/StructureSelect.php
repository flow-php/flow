<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\{EntryReference, Reference, References};

final readonly class StructureSelect implements ScalarFunction
{
    private Reference $ref;

    private References $refs;

    public function __construct(
        Reference|string $ref,
        Reference|string ...$refs,
    ) {
        $this->ref = EntryReference::init($ref);
        $this->refs = References::init(...$refs);
    }

    public function eval(Row $row) : ?array
    {
        if (!$row->has($this->ref)) {
            return null;
        }

        $structure = $row->get($this->ref);

        if (!$structure instanceof StructureEntry) {
            return null;
        }

        $output = [];

        foreach ($this->refs as $ref) {
            if (\array_key_exists($ref->to(), $structure->value() ?: [])) {
                $output[$ref->name()] = $structure->value() ? $structure->value()[$ref->to()] : null;
            } else {
                $output[$ref->name()] = null;
            }
        }

        return $output;
    }
}
