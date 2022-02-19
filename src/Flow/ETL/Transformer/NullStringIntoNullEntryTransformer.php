<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @psalm-immutable
 */
final class NullStringIntoNullEntryTransformer implements Transformer
{
    /**
     * @var string[]
     */
    private array $entryNames;

    public function __construct(string ...$entryNames)
    {
        $this->entryNames = $entryNames;
    }

    /**
     * @return array{entry_names: array<string>}
     */
    public function __serialize() : array
    {
        return [
            'entry_names' => $this->entryNames,
        ];
    }

    /**
     * @param array{entry_names: array<string>} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->entryNames = $data['entry_names'];
    }

    public function transform(Rows $rows) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         */
        $transformer = function (Row $row) : Row {
            foreach ($this->entryNames as $entryName) {
                $entry = $row->get($entryName);

                if (!\is_string($entry->value())) {
                    continue;
                }

                if (\mb_strtolower($entry->value()) === 'null') {
                    $row = $row->set(new Row\Entry\NullEntry($entry->name()));
                }
            }

            return $row;
        };

        return $rows->map($transformer);
    }
}
