<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row;
use Flow\ETL\Schema;

use function json_encode;

use const JSON_THROW_ON_ERROR;

final class RowPadding
{
    /**
     * @param array<int, string> $order file-schema column names, in order
     */
    private function __construct(
        private readonly array $order,
    ) {}

    public static function forFileSchema(Schema $fileSchema, SchemaDecoder $decoder): self
    {
        $order = [];

        foreach ($decoder->decode(json_encode($fileSchema->normalize(), JSON_THROW_ON_ERROR)) as $column) {
            $order[] = $column->name;
        }

        return new self($order);
    }

    public function apply(Row $row): Row
    {
        $values = $row->values();
        $padded = [];

        foreach ($this->order as $name) {
            $padded[$name] = $values[$name] ?? null;
        }

        return new Row($padded);
    }
}
