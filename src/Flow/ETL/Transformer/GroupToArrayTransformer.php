<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{group_by_entry: string, new_entry_name: string}>
 * @psalm-immutable
 */
final class GroupToArrayTransformer implements Transformer
{
    public function __construct(private readonly string $groupByEntry, private readonly string $newEntryName)
    {
    }

    public function __serialize() : array
    {
        return [
            'group_by_entry' => $this->groupByEntry,
            'new_entry_name' => $this->newEntryName,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->groupByEntry = $data['group_by_entry'];
        $this->newEntryName = $data['new_entry_name'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /** @var array<array-key, array<mixed>> $entries */
        $entries = [];

        foreach ($rows as $row) {
            /** @var array<array-key, array<mixed>> $entries */
            $groupValue = $row->get($this->groupByEntry)->toString();

            if (!\array_key_exists($groupValue, $entries)) {
                $entries[$groupValue] = [];
            }

            $entries[$groupValue][] = $row->toArray();
        }

        $rows = new Rows();

        /** @var array<mixed> $entry */
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
