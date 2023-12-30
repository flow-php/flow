<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;

final class StructureSelect implements ScalarFunction
{
    private readonly Reference $ref;

    private readonly References $refs;

    public function __construct(
        Reference|string $ref,
        Reference|string ...$refs,
    ) {
        $this->ref = EntryReference::init($ref);
        $this->refs = References::init(...$refs);
    }

    public function eval(Row $row) : array|null
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
            if (\array_key_exists($ref->to(), $structure->value())) {
                $output[$ref->name()] = $structure->value()[$ref->to()];
            } else {
                $output[$ref->name()] = null;
            }
        }

        return $output;
    }
}
