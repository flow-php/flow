<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Factory;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;
use Flow\ETL\RowsFactory;
use Flow\ETL\Transformer\ArrayUnpackTransformer;
use Flow\ETL\Transformer\RemoveEntriesTransformer;

/**
 * @implements RowsFactory<array{schema: ?Schema}>
 */
final class ArrayRowsFactory implements RowsFactory
{
    public function __construct(private readonly ?Schema $schema = null)
    {
    }

    public function __serialize() : array
    {
        return [
            'schema' => $this->schema,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->schema = $data['schema'];
    }

    /**
     * @param array<array<mixed>> $data
     *
     * @throws InvalidArgumentException
     * @throws \Flow\ETL\Exception\RuntimeException
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
            (new ArrayUnpackTransformer('element', entryFactory: new NativeEntryFactory($this->schema)))
                ->transform(new Rows(...\array_map(
                    fn (array $row) : Row => Row::create(new Row\Entry\ArrayEntry('element', $row)),
                    $data
                )))
        );
    }
}
