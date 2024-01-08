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
    private array $refs;

    /**
     * @var array<string>
     */
    private array $without = [];

    public function __construct(string|Reference ...$reference)
    {
        $refs = [];

        foreach ($reference as $ref) {
            $refs[] = EntryReference::init($ref);
        }

        $indexedRefs = [];

        foreach ($refs as $ref) {
            $indexedRefs[$ref->name()] = $ref;
        }

        $this->refs = $indexedRefs;
    }

    public static function init(string|Reference ...$reference) : self
    {
        return new self(...$reference);
    }

    public function add(string|Reference $ref) : self
    {
        $reference = EntryReference::init($ref);

        if (\in_array($reference->name(), $this->without, true)) {
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
        /**
         * @var array<string>
         */
        $without = [];

        foreach ($reference as $ref) {
            $without[] = $ref instanceof Reference ? $ref->name() : $ref;
        }

        $this->without = \array_values(\array_unique(\array_merge($this->without, $without)));

        $keepReferences = [];

        foreach ($this->refs as $refName => $ref) {
            if (\in_array($refName, $without, true)) {
                continue;
            }

            $keepReferences[$ref->name()] = $ref;
        }

        $this->refs = $keepReferences;

        return $this;
    }
}
