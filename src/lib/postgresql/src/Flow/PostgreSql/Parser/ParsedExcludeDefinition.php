<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Parser;

use function array_map;

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

    public function normalized(ExpressionParser $parser): self
    {
        return new self(
            $this->accessMethod,
            array_map(static fn(array $element): array => [
                'expression' => $parser->normalize($element['expression']),
                'operator' => $element['operator'],
            ], $this->elements),
            $this->predicate !== null ? $parser->normalize($this->predicate) : null,
            $this->deferrable,
            $this->initiallyDeferred,
        );
    }

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
