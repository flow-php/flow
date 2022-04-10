<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{names: array<string>}>
 * @psalm-immutable
 */
final class KeepEntriesTransformer implements Transformer
{
    /**
     * @var string[]
     */
    private readonly array $names;

    public function __construct(string ...$names)
    {
        $this->names = $names;
    }

    public function __serialize() : array
    {
        return [
            'names' => $this->names,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->names = $data['names'];
    }

    public function transform(Rows $rows) : Rows
    {
        /** @psalm-var pure-callable(Row) : Row $transformer */
        $transformer = function (Row $row) : Row {
            $allEntries = $row->entries()->map(fn (Entry $entry) : string => $entry->name());
            $removeEntries = \array_diff($allEntries, $this->names);

            return $row->remove(...$removeEntries);
        };

        return $rows->map($transformer);
    }
}
