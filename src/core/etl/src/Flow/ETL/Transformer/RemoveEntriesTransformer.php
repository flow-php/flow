<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{names: array<string>}>
 */
final class RemoveEntriesTransformer implements Transformer
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

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         */
        $transformer = fn (Row $row) : Row => $row->remove(...$this->names);

        return $rows->map($transformer);
    }
}
