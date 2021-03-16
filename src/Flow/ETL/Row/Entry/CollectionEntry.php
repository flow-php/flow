<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayWeakComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;

/**
 * @psalm-immutable
 */
final class CollectionEntry implements Entry
{
    private string $key;

    private string $name;

    /**
     * @var Entries[]
     */
    private array $entries;

    public function __construct(string $name, Entries ...$entries)
    {
        if (empty($name)) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->key = \mb_strtolower($name);
        $this->name = $name;
        $this->entries = $entries;
    }

    public function append(Entries $entries) : self
    {
        return new self($this->name, ...[...$this->entries, $entries]);
    }

    /**
     * @psalm-suppress MissingClosureReturnType
     * @psalm-suppress ImpureMethodCall
     */
    public function entryFromAll(string $name) : Entry
    {
        $entries = \array_unique($this->mapEntries(fn (Entries $entries) => $entries->get($name)->value()));

        if (\count($entries) !== 1) {
            throw InvalidArgumentException::because(
                \sprintf(
                    'Entry "%s" has different values in "%s" collection entry: [%s]',
                    $name,
                    $this->name,
                    \implode(', ', $entries)
                )
            );
        }

        /** @phpstan-ignore-next-line */
        return \current($this->entries)->get($name);
    }

    /**
     * @psalm-suppress MixedArgument
     */
    public function removeFromAll(string $name) : self
    {
        return new self(
            $this->name,
            ...$this->mapEntries(fn (Entries $entries) : Entries => $entries->remove($name))
        );
    }

    public function name() : string
    {
        return $this->name;
    }

    /**
     * @psalm-suppress MissingReturnType
     * @phpstan-ignore-next-line
     */
    public function value() : array
    {
        return $this->mapEntries(fn (Entries $entries) : array => $entries->toArray());
    }

    /**
     * @psalm-suppress InvalidArgument
     */
    public function is(string $name) : bool
    {
        return $this->key === \mb_strtolower($name);
    }

    public function rename(string $name) : Entry
    {
        return new self($name, ...$this->entries);
    }

    /**
     * @psalm-param pure-callable(Entries) : bool $filter
     */
    public function filterEntries(callable $filter) : self
    {
        return new self($this->name, ...\array_filter($this->entries, $filter));
    }

    /**
     * @psalm-suppress MixedArgument
     */
    public function map(callable $mapper) : Entry
    {
        return new self($this->name, ...$mapper($this->entries));
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self && (new ArrayWeakComparison())->equals($this->value(), $entry->value());
    }

    /**
     * @psalm-suppress ImpureFunctionCall
     * @phpstan-ignore-next-line
     */
    private function mapEntries(callable $callable) : array
    {
        return \array_map($callable, $this->entries);
    }
}
