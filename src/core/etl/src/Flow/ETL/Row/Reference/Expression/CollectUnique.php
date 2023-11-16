<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;

final class CollectUnique implements Aggregator
{
    /**
     * @var array<mixed>
     */
    private array $collection;

    public function __construct(private readonly Reference $ref)
    {
        $this->collection = [];
    }

    public function aggregate(Row $row) : void
    {
        try {
            /** @var array<string, mixed> $values */
            $values = [];

            if ($this->ref instanceof Row\StructureReference) {
                foreach ($this->ref->to() as $ref) {
                    $values[$ref->name()] = $row->valueOf($ref);
                }
            } else {
                /**
                 * @psalm-suppress InvalidArgument
                 *
                 * @phpstan-ignore-next-line
                 */
                $values[$this->ref->name()] = $row->valueOf($this->ref);
            }

            if ($this->ref instanceof EntryReference) {
                /** @var mixed $value */
                $value = \current($values);

                if (!\in_array($value, $this->collection, true)) {
                    $this->collection[] = $value;
                }
            } else {
                if (!\in_array($values, $this->collection, true)) {
                    $this->collection[] = $values;
                }
            }
        } catch (InvalidArgumentException) {
            // do nothing?
        }
    }

    public function result() : Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->name() . '_collection_unique');
        }

        return \Flow\ETL\DSL\Entry::array($this->ref->name(), $this->collection);
    }
}
