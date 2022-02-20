<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Factory;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\RowsFactory;
use Flow\ETL\Transformer\ArrayUnpackTransformer;
use Flow\ETL\Transformer\RemoveEntriesTransformer;

final class ArrayRowsFactory implements RowsFactory
{
    /**
     * @return array<string, mixed>
     */
    public function __serialize() : array
    {
        return [];
    }

    /**
     * @param array<string, mixed> $data
     */
    public function __unserialize(array $data) : void
    {
    }

    /**
     * @param array<array<mixed>> $data
     *
     * @throws InvalidArgumentException
     * @throws \Flow\ETL\Exception\RuntimeException
     *
     * @return Rows
     */
    public function create(array $data) : Rows
    {
        foreach ($data as $row) {
            /** @psalm-suppress DocblockTypeContradiction */
            if (!\is_array($row)) {
                throw new InvalidArgumentException('ArrayRowsFactory expects data to be an array of arrays');
            }
        }

        return (new RemoveEntriesTransformer('element'))->transform(
            (new ArrayUnpackTransformer('element'))->transform(new Rows(...\array_map(
                function (array $row) : Row {
                    return Row::create(new Row\Entry\ArrayEntry('element', $row));
                },
                $data
            )))
        );
    }
}
