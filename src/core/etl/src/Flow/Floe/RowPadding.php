<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;
use Flow\ETL\Schema;

use function json_encode;

use const JSON_THROW_ON_ERROR;

final class RowPadding
{
    /**
     * @param array<int, string> $order file-schema column names, in order
     * @param array<string, Entry<mixed>> $nulls shared null entry per column, used for absent columns
     */
    private function __construct(
        private readonly array $order,
        private readonly array $nulls,
    ) {}

    public static function forFileSchema(Schema $fileSchema, SchemaDecoder $decoder): self
    {
        $order = [];
        $nulls = [];

        foreach ($decoder->decode(json_encode($fileSchema->normalize(), JSON_THROW_ON_ERROR)) as $column) {
            $order[] = $column->name;
            $nulls[$column->name] = $column->instantiator->instantiate(
                $column->name,
                null,
                $column->definition->makeNullable(),
            );
        }

        return new self($order, $nulls);
    }

    public function apply(Row $row): Row
    {
        $byName = [];

        foreach ($row->entries()->all() as $entry) {
            $byName[$entry->name()] = $entry;
        }

        $entries = [];

        foreach ($this->order as $name) {
            $entries[$name] = $byName[$name] ?? $this->nulls[$name];
        }

        return new Row(Entries::recreate($entries));
    }
}
