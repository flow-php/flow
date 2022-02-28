<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @psalm-immutable
 */
final class RemoveEntriesTransformer implements Transformer
{
    /**
     * @var string[]
     */
    private array $names;

    public function __construct(string ...$names)
    {
        $this->names = $names;
    }

    /**
     * @return array{names: array<string>}
     */
    public function __serialize() : array
    {
        return [
            'names' => $this->names,
        ];
    }

    /**
     * @param array{names: array<string>} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->names = $data['names'];
    }

    public function transform(Rows $rows) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         */
        $transformer = function (Row $row) : Row {
            return $row->remove_entries(...$this->names);
        };

        return $rows->map($transformer);
    }
}
