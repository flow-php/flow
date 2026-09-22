<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionFactory;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\CreateTriggerFinalStep;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\TriggerEvent as QbTriggerEvent;

use function array_map;
use function Flow\PostgreSql\DSL\create;

/**
 * @type TriggerShape = array{name: string, table_name: string, timing: string, events: non-empty-list<string>, function_name: string, for_each_row: bool, when_condition: ?string}
 */
final readonly class Trigger
{
    private ?string $whenConditionKey;

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
        $this->whenConditionKey = $whenCondition !== null ? (new ExpressionParser())->normalize($whenCondition) : null;
    }

    /**
     * @param TriggerShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: $data['name'],
            tableName: $data['table_name'],
            timing: TriggerTiming::from($data['timing']),
            events: array_map(static fn(string $event): TriggerEvent => TriggerEvent::from($event), $data['events']),
            functionName: $data['function_name'],
            forEachRow: $data['for_each_row'],
            whenCondition: $data['when_condition'] ?? null,
        );
    }

    public function isEqual(self $other): bool
    {
        return (
            $this->name === $other->name
            && $this->tableName === $other->tableName
            && $this->isEqualStructure($other)
        );
    }

    public function isEqualStructure(self $other): bool
    {
        return (
            $this->timing === $other->timing
            && $this->events === $other->events
            && $this->functionName === $other->functionName
            && $this->forEachRow === $other->forEachRow
            && $this->whenConditionKey === $other->whenConditionKey
        );
    }

    public function toSql(string $tableName, string $schema): CreateTriggerFinalStep
    {
        $triggerBuilder = create()->trigger($this->name);
        $events = array_map(static fn(TriggerEvent $e): QbTriggerEvent => QbTriggerEvent::{$e->name}, $this->events);

        $onStep = match ($this->timing) {
            TriggerTiming::BEFORE => $triggerBuilder->before(...$events),
            TriggerTiming::AFTER => $triggerBuilder->after(...$events),
            TriggerTiming::INSTEAD_OF => $triggerBuilder->insteadOf(...$events),
        };
        $optionsStep = $onStep->on($tableName, $schema);

        if ($this->forEachRow) {
            $optionsStep = $optionsStep->forEachRow();
        }

        if ($this->whenCondition !== null) {
            $optionsStep = $optionsStep->when(ConditionFactory::fromAst((new ExpressionParser())->parse($this->whenCondition)));
        }

        return $optionsStep->execute($this->functionName);
    }

    public function whenConditionKey(): ?string
    {
        return $this->whenConditionKey;
    }

    public function withFunctionSchema(string $schema): self
    {
        if (QualifiedIdentifier::parse($this->functionName)->count() > 1) {
            return $this;
        }

        return new self(
            $this->name,
            $this->tableName,
            $this->timing,
            $this->events,
            $schema . '.' . $this->functionName,
            $this->forEachRow,
            $this->whenCondition,
        );
    }

    /**
     * @return TriggerShape
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'table_name' => $this->tableName,
            'timing' => $this->timing->value,
            'events' => array_map(static fn(TriggerEvent $event): string => $event->value, $this->events),
            'function_name' => $this->functionName,
            'for_each_row' => $this->forEachRow,
            'when_condition' => $this->whenCondition,
        ];
    }
}
