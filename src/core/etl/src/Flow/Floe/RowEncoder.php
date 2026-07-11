<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Schema\Metadata;

use function count;

final class RowEncoder
{
    public function encode(EncoderPlan $plan, Row $row): string
    {
        $entries = $row->entries()->all();

        // fast path: the row's columns align positionally with the plan (every row
        // of a homogeneous batch), so no per-row name-keyed map is needed
        if (count($entries) === count($plan->columns)) {
            $body = '';
            $index = 0;

            foreach ($plan->columns as $column) {
                $entry = $entries[$index++];

                if ($entry->name() !== $column->name) {
                    return $this->encodeByName($plan, $entries);
                }

                $body .= $this->entryBytes($column, $entry);
            }

            return $body;
        }

        return $this->encodeByName($plan, $entries);
    }

    /**
     * @param array<array-key, Entry<mixed>> $entries
     */
    private function encodeByName(EncoderPlan $plan, array $entries): string
    {
        $keyed = [];

        foreach ($entries as $entry) {
            $keyed[$entry->name()] = $entry;
        }

        $body = '';

        foreach ($plan->columns as $column) {
            $entry = $keyed[$column->name] ?? null;

            $body .= $entry === null ? Format::VALUE_ABSENT_BYTE : $this->entryBytes($column, $entry);
        }

        return $body;
    }

    /**
     * @param Entry<mixed> $entry
     */
    private function entryBytes(EncoderColumn $column, Entry $entry): string
    {
        // @mago-ignore analysis:mixed-assignment
        $value = $entry->value();

        if ($value === null) {
            return $entry->definition()->metadata()->has(Metadata::FROM_NULL)
                ? Format::VALUE_NULL_FROM_NULL_BYTE
                : Format::VALUE_NULL_BYTE;
        }

        return Format::VALUE_PRESENT_BYTE . $column->encoder->encode($value);
    }
}
