<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Reference;

use function current;
use function in_array;

final class CollectUnique implements AggregatingFunction
{
    /**
     * @var array<mixed>
     */
    private array $collection;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->collection = [];
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        if (!$row->has($this->ref)) {
            return;
        }

        try {
            /** @var array<string, mixed> $values */
            $values = [];

            $values[$this->ref->name()] = $row->valueOf($this->ref);

            /** @var mixed $value */
            $value = current($values);

            if (!in_array($value, $this->collection, true)) {
                $this->collection[] = $value;
            }
        } catch (InvalidArgumentException $e) {
            $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('CollectUnique error: ' . $e->getMessage()));
        }
    }

    /**
     * @return Entry<mixed>
     */
    /**
     * @return list<Reference>
     */
    public function references(): array
    {
        return [$this->ref];
    }

    public function result(EntryFactory $entryFactory): Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->name() . '_collection_unique');
        }

        return $entryFactory->create($this->ref->name(), $this->collection);
    }
}
