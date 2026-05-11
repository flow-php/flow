<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Parser;

final readonly class ParsedExcludeDefinition
{
    /**
     * @param list<array{expression: string, operator: string}> $elements
     */
    public function __construct(
        public string $accessMethod,
        public array $elements,
        public ?string $predicate,
        public bool $deferrable,
        public bool $initiallyDeferred,
    ) {}

    public function equals(self $other): bool
    {
        return (
            $this->accessMethod === $other->accessMethod
            && $this->elements === $other->elements
            && $this->predicate === $other->predicate
            && $this->deferrable === $other->deferrable
            && $this->initiallyDeferred === $other->initiallyDeferred
        );
    }
}
