<?php

declare(strict_types=1);

namespace Flow\ETL\Partition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Partition;
use Flow\ETL\Serializer\Closure;
use Laravel\SerializableClosure\SerializableClosure;

/**
 * @implements PartitionFilter<array{filter: SerializableClosure}>
 */
final class CallableFilter implements PartitionFilter
{
    /**
     * @var callable(Partition ...$partition) : bool
     */
    private $filter;

    /**
     * @param callable(Partition ...$partition) : bool $filter
     */
    public function __construct(callable $filter)
    {
        $this->filter = $filter;
    }

    public function __serialize() : array
    {
        if (!Closure::isSerializable()) {
            throw new RuntimeException('CallbackEntryTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        return [
            'filter' => new SerializableClosure(\Closure::fromCallable($this->filter)),
        ];
    }

    public function __unserialize(array $data) : void
    {
        if (!Closure::isSerializable()) {
            throw new RuntimeException('CallbackEntryTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        $this->filter = $data['filter']->getClosure();
    }

    public function keep(Partition ...$partitions) : bool
    {
        return ($this->filter)(...$partitions);
    }
}
