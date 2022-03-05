<?php

declare(strict_types=1);

namespace Flow\ETL\Factory;

use Flow\ETL\Row\RowConverter;
use Flow\ETL\Rows;
use Flow\ETL\RowsFactory;
use Flow\ETL\Transformer\CastTransformer;

/**
 * @implements RowsFactory<array{factory: RowsFactory, cast_entries: array<RowConverter>}>
 */
final class CastedRowsFactory implements RowsFactory
{
    /**
     * @var array<RowConverter>
     */
    private array $castEntries;

    private RowsFactory $factory;

    public function __construct(RowsFactory $factory, RowConverter ...$castEntries)
    {
        $this->factory = $factory;
        $this->castEntries = $castEntries;
    }

    public function __serialize() : array
    {
        return [
            'factory' => $this->factory,
            'cast_entries' => $this->castEntries,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->factory = $data['factory'];
        $this->castEntries = $data['cast_entries'];
    }

    /**
     * @param array<array<mixed>> $data
     *
     * @return Rows
     */
    public function create(array $data) : Rows
    {
        return (new CastTransformer(...$this->castEntries))->transform($this->factory->create($data));
    }
}
