<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class GroupToArrayTransformer implements Transformer
{
    public function __construct(private readonly string $groupByEntry, private readonly string $newEntryName)
    {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /** @var array<array-key, array<mixed>> $entries */
        $entries = [];

        foreach ($rows as $row) {
            $groupValue = $row->get($this->groupByEntry)->toString();

            if (!\array_key_exists($groupValue, $entries)) {
                $entries[$groupValue] = [];
            }

            $entries[$groupValue][] = $row->toArray();
        }

        $rows = new Rows();

        foreach ($entries as $entry) {
            $rows = $rows->add(
                Row::create(
                    new Entry\ArrayEntry(
                        $this->newEntryName,
                        $entry
                    )
                )
            );
        }

        return $rows;
    }
}
