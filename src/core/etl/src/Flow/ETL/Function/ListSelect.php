<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;

final class ListSelect implements ScalarFunction
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

        $list = $row->get($this->ref);

        if (!$list instanceof ListEntry) {
            return null;
        }

        $output = [];

        foreach ($list->value() as $index => $element) {
            $output[$index] = [];

            foreach ($this->refs as $ref) {
                if (\is_array($element) && \array_key_exists($ref->to(), $element)) {
                    $output[$index][$ref->name()] = $element[$ref->to()];
                } else {
                    $output[$index][$ref->name()] = null;
                }
            }
        }

        return $output;
    }
}
