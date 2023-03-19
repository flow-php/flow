<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Serializer\Serializable;
use Traversable;

/**
 * @implements \ArrayAccess<string, EntryReference>
 * @implements \IteratorAggregate<string, EntryReference>
 * @implements Serializable<array{refs: array<string, EntryReference>}>
 */
final class References  implements \ArrayAccess, \Countable, \IteratorAggregate, Serializable
{
    /**
     * @var array<string, EntryReference>
     */
    private readonly array $refs;

    public function __construct(string|Reference ...$reference)
    {
        $refs = [];
        foreach (EntryReference::initAll(...$reference) as $ref) {
            $refs[$ref->name()] = $ref;
        }

        $this->refs = $refs;
    }

    public static function init(string|Reference ...$reference) : self
    {
        return new self(...$reference);
    }

    public function has(string|Reference $reference) : bool
    {
        foreach ($this->refs as $ref) {
            if ($ref->is($reference)) {
                return true;
            }
        }

        return false;
    }

    public function getIterator(): Traversable
    {
        return new \ArrayIterator($this->refs);
    }

    public function offsetExists($offset): bool
    {
        if (!$offset instanceof Reference) {
            return false;
        }

        return \array_key_exists($offset->name(), $this->refs);
    }

    public function offsetGet($offset): EntryReference
    {
        if ($this->offsetExists($offset)) {
            return $this->refs[$offset->name()];
        }

        throw new InvalidArgumentException("Row {$offset} does not exists.");
    }

    public function offsetSet(mixed $offset, mixed $value): void
    {

    }

    public function offsetUnset(mixed $offset): void
    {
    }

    public function count(): int
    {
        return \count($this->refs);
    }

    public function __serialize(): array
    {
        return [
            'refs' => $this->refs
        ];
    }

    public function __unserialize(array $data): void
    {
        $this->refs = $data['refs'];
    }
}