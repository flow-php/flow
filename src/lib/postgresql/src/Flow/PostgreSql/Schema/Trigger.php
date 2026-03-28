<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

final readonly class Trigger
{
    /**
     * @param non-empty-list<TriggerEvent> $events
     */
    public function __construct(
        public string $name,
        public string $tableName,
        public TriggerTiming $timing,
        public array $events,
        public string $functionName,
        public bool $forEachRow = false,
        public ?string $whenCondition = null,
    ) {
    }

    public function isEqual(self $other) : bool
    {
        return $this->name === $other->name
            && $this->tableName === $other->tableName
            && $this->isEqualStructure($other);
    }

    public function isEqualStructure(self $other) : bool
    {
        return $this->timing === $other->timing
            && $this->events === $other->events
            && $this->functionName === $other->functionName
            && $this->forEachRow === $other->forEachRow
            && $this->whenCondition === $other->whenCondition;
    }
}
