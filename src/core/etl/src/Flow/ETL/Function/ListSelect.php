<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;

use function array_key_exists;
use function is_array;

final readonly class ListSelect implements ScalarFunction
{
    private Reference $ref;

    private References $refs;

    public function __construct(Reference|string $ref, Reference|string ...$refs)
    {
        $this->ref = EntryReference::init($ref);
        $this->refs = References::init(...$refs);
    }

    /**
     * @return null|array<int, array<string, mixed>>
     */
    public function eval(Row $row, FlowContext $context): ?array
    {
        if (!$row->has($this->ref)) {
            return null;
        }

        $list = $row->get($this->ref);

        if (!$list instanceof ListEntry) {
            return null;
        }

        $output = [];

        foreach ($list->value() ?: [] as $index => $element) {
            $output[$index] = [];

            foreach ($this->refs as $ref) {
                if (is_array($element) && array_key_exists($ref->to(), $element)) {
                    $output[$index][$ref->name()] = $element[$ref->to()];
                } else {
                    $output[$index][$ref->name()] = null;
                }
            }
        }

        return $output;
    }
}
