<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Function\Parameter;
use Flow\ETL\{FlowContext, Rows, Transformer, WithEntry};

final readonly class DuplicateRowTransformer implements Transformer
{
    /**
     * @var array<WithEntry>
     */
    private array $entries;

    /**
     * @param mixed $condition
     * @param WithEntry ...$entries
     */
    public function __construct(
        private mixed $condition,
        WithEntry ...$entries,
    ) {
        $this->entries = $entries;
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $duplicatedRows = \Flow\ETL\DSL\rows();

        foreach ($rows->all() as $row) {
            $condition = (new Parameter($this->condition))->asBoolean($row);

            if ($condition) {
                $duplicatedRow = \Flow\ETL\DSL\rows($row->duplicate());

                foreach ($this->entries as $entry) {
                    $duplicatedRow = (new ScalarFunctionTransformer($entry->name, $entry->function))->transform($duplicatedRow, $context);
                }

                $duplicatedRows = $duplicatedRows->merge($duplicatedRow);
            }
        }

        if ($duplicatedRows->count()) {
            $rows = $rows->merge($duplicatedRows);
        }

        return $rows;
    }
}
