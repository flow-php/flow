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

final class Collect implements AggregatingFunction
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

            $this->collection[] = current($values);
        } catch (InvalidArgumentException $e) {
            throw new InvalidArgumentException('Collect error: ' . $e->getMessage(), 0, $e);
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
        $ref = $this->ref->hasAlias() ? $this->ref : $this->ref->as($this->ref->name() . '_collection');

        return $entryFactory->create($ref->name(), $this->collection);
    }
}
