<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

/**
 * What a layout prints: a numbered name with its details already rendered. Every later visit of the same source is a
 * shared reference with no children.
 */
final readonly class Entry
{
    /**
     * @param object $source what this entry describes, the identity a layout reaches it by
     * @param list<string> $lines the details under the title
     * @param list<Entry> $children
     * @param string $suffix what goes on the title line, after the name
     */
    public function __construct(
        public object $source,
        public string $name,
        public array $lines,
        public ?int $number,
        public bool $shared,
        public array $children,
        public string $suffix = '',
    ) {}

    /**
     * The name with the number in front; an entry without a number - Outputs, which only groups the consumers - has
     * none.
     */
    public function title(): string
    {
        return $this->number === null ? $this->name : '#' . $this->number . ' ' . $this->name;
    }

    /**
     * The same entry read from somewhere else in the plan: shared drops what the first visit already printed.
     *
     * @param list<Entry> $children
     */
    public function with(array $children, bool $shared = false): self
    {
        return new self(
            $this->source,
            $this->name,
            $shared ? [] : $this->lines,
            $this->number,
            $shared,
            $children,
            $shared ? '' : $this->suffix,
        );
    }
}
