<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{Exception\InvalidArgumentException,
    FlowContext,
    Row,
    Rows,
    Transformer,
    Transformer\Rename\RenameEntryStrategy};

final readonly class RenameEachEntryTransformer implements Transformer
{
    /**
     * @var array<RenameEntryStrategy>
     */
    private array $strategies;

    public function __construct(RenameEntryStrategy ...$strategies)
    {
        if ([] === $strategies) {
            throw new InvalidArgumentException('At least one strategy must be provided.');
        }

        $this->strategies = $strategies;
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(function (Row $row) use ($context) : Row {
            foreach ($this->strategies as $strategy) {
                foreach ($row->entries()->all() as $entry) {
                    $row = $strategy->rename($row, $entry, $context);
                }
            }

            return $row;
        });
    }
}
