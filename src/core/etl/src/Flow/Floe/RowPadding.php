<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;
use Flow\ETL\Schema;

use function array_values;
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

        foreach (array_values($fileSchema->definitions()) as $definition) {
            $name = $definition->entry()->name();
            $order[] = $name;
            $column = $decoder->decode(json_encode([$definition->normalize()], JSON_THROW_ON_ERROR))[0];
            $nulls[$name] = $column->instantiator->instantiate($name, null, clone $column->fromNullDefinition);
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
