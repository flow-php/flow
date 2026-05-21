<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;

use function array_key_exists;

final readonly class StructureSelect implements ScalarFunction
{
    private Reference $ref;

    private References $refs;

    public function __construct(Reference|string $ref, Reference|string ...$refs)
    {
        $this->ref = EntryReference::init($ref);
        $this->refs = References::init(...$refs);
    }

    /**
     * @return null|array<string, mixed>
     */
    public function eval(Row $row, FlowContext $context): ?array
    {
        if (!$row->has($this->ref)) {
            return null;
        }

        $structure = $row->get($this->ref);

        if (!$structure instanceof StructureEntry) {
            return null;
        }

        $value = $structure->value();
        $output = [];

        foreach ($this->refs as $ref) {
            if ($value !== null && array_key_exists($ref->to(), $value)) {
                $output[$ref->name()] = $value[$ref->to()];
            } else {
                $output[$ref->name()] = null;
            }
        }

        return $output;
    }
}
