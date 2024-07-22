<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;

/**
 * @implements \ArrayAccess<string, Reference>
 * @implements \IteratorAggregate<string, Reference>
 */
final class References implements \ArrayAccess, \Countable, \IteratorAggregate
{
    /**
     * @var array<string, Reference>
     */
    private array $refs = [];

    /**
     * @var array<string, true>
     */
    private array $without = [];

    public function __construct(string|Reference ...$references)
    {
        foreach ($references as $ref) {
            $ref = EntryReference::init($ref);

            $this->refs[$ref->name()] = $ref;
        }
    }

    public static function init(string|Reference ...$references) : self
    {
        return new self(...$references);
    }

    public function add(string|Reference $ref) : self
    {
        $reference = EntryReference::init($ref);

        if (\array_key_exists($reference->name(), $this->without)) {
            return $this;
        }

        $this->refs[$reference->name()] = $reference;

        return $this;
    }

    /**
     * @return array<Reference>
     */
    public function all() : array
    {
        return \array_values($this->refs);
    }

    public function count() : int
    {
        return \count($this->refs);
    }

    public function first() : Reference
    {
        if (!\count($this->refs)) {
            throw new InvalidArgumentException('References are empty.');
        }

        return \current($this->refs);
    }

    /**
     * @return \Traversable<string, Reference>
     */
    public function getIterator() : \Traversable
    {
        return new \ArrayIterator($this->refs);
    }

    public function has(string|Reference $reference) : bool
    {
        $reference = EntryReference::init($reference);

        foreach ($this->refs as $ref) {
            if ($ref->is($reference)) {
                return true;
            }
        }

        return false;
    }

    /**
     * @return array<string>
     */
    public function names() : array
    {
        $names = [];

        foreach ($this->refs as $ref) {
            $names[] = $ref->name();
        }

        return $names;
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
     * @return Reference
     */
    public function offsetGet($offset) : Reference
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

    public function without(string|Reference ...$reference) : self
    {
        foreach ($reference as $ref) {
            $refName = $ref instanceof Reference ? $ref->name() : $ref;

            $this->without[$refName] = true;

            if (\array_key_exists($refName, $this->refs)) {
                unset($this->refs[$refName]);
            }
        }

        return $this;
    }
}
