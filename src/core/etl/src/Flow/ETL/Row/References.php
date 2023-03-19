<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Serializer\Serializable;

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

    public function __serialize() : array
    {
        return [
            'refs' => $this->refs,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->refs = $data['refs'];
    }

    /**
     * @return array<EntryReference>
     */
    public function all() : array
    {
        return $this->refs;
    }

    public function count() : int
    {
        return \count($this->refs);
    }

    /**
     * @return \Traversable<string, EntryReference>
     */
    public function getIterator() : \Traversable
    {
        return new \ArrayIterator($this->refs);
    }

    public function has(string|EntryReference $reference) : bool
    {
        foreach ($this->refs as $ref) {
            if ($ref->is(EntryReference::init($reference))) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param string $offset
     *
     * @return bool
     */
    public function offsetExists($offset) : bool
    {
        return \array_key_exists($offset, $this->refs);
    }

    /**
     * @param string $offset
     *
     * @throws InvalidArgumentException
     *
     * @return EntryReference
     */
    public function offsetGet($offset) : EntryReference
    {
        if ($this->offsetExists($offset)) {
            return $this->refs[$offset];
        }

        throw new InvalidArgumentException("Reference {$offset} does not exists.");
    }

    public function offsetSet(mixed $offset, mixed $value) : void
    {
        throw new InvalidArgumentException('Method not implemented.');
    }

    public function offsetUnset(mixed $offset) : void
    {
        throw new InvalidArgumentException('Method not implemented.');
    }

    public function reverse() : self
    {
        return new self(...\array_reverse($this->refs));
    }
}
