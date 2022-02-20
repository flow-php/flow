<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Factory;

use Flow\ETL\Row\RowConverter;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\CastTransformer;
use Flow\ETL\Transformer\RowsFactory;

final class CastedRowsFactory implements RowsFactory
{
    private RowsFactory $factory;

    /**
     * @var array<RowConverter>
     */
    private array $castEntries;

    public function __construct(RowsFactory $factory, RowConverter ...$castEntries)
    {
        $this->factory = $factory;
        $this->castEntries = $castEntries;
    }

    /**
     * @return array{factory: RowsFactory, cast_entries: array<RowConverter>}
     */
    public function __serialize() : array
    {
        return [
            'factory' => $this->factory,
            'cast_entries' => $this->castEntries,
        ];
    }

    /**
     * @psalm-suppress MoreSpecificImplementedParamType
     *
     * @param array{factory: RowsFactory, cast_entries: array<RowConverter>} $data
     */
    public function __unserialize(array $data) : void
    {
        $this->factory = $data['factory'];
        $this->castEntries = $data['cast_entries'];
    }

    /** @phpstan-ignore-next-line */
    public function create(array $data) : Rows
    {
        return (new CastTransformer(...$this->castEntries))->transform($this->factory->create($data));
    }
}
