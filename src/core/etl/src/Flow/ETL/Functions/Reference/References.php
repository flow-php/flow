<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Reference;

use ArrayAccess;
use ArrayIterator;
use Countable;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use IteratorAggregate;
use SensitiveParameter;
use Traversable;

use function array_key_exists;
use function array_keys;
use function array_reverse;
use function array_values;
use function count;
use function current;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

/**
 * @implements \ArrayAccess<string, Reference>
 * @implements \IteratorAggregate<string, Reference>
 */
final class References implements ArrayAccess, Countable, Describable, IteratorAggregate
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
            $ref = UnresolvedReference::init($ref);

            $this->refs[$ref->name()] = $ref;
        }
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return (new self(...$rebuild->all($data['refs'] ?? null, Reference::class)))->without(
            ...type_list(type_string())->assert($data['without'] ?? null),
        );
    }

    public static function init(string|Reference ...$references): self
    {
        return new self(...$references);
    }

    public function add(string|Reference $ref): self
    {
        $reference = UnresolvedReference::init($ref);

        if (array_key_exists($reference->name(), $this->without)) {
            return $this;
        }

        $this->refs[$reference->name()] = $reference;

        return $this;
    }

    /**
     * @return list<Reference>
     */
    public function all(): array
    {
        return array_values($this->refs);
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'refs' => array_values($this->refs),
            'without' => array_keys($this->without),
        ]);
    }

    public function count(): int
    {
        return count($this->refs);
    }

    public function first(): Reference
    {
        if (!count($this->refs)) {
            throw new InvalidArgumentException('References are empty.');
        }

        $first = current($this->refs);

        if ($first === false) {
            throw new InvalidArgumentException('References are empty.');
        }

        return $first;
    }

    /**
     * @return \Traversable<string, Reference>
     */
    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->refs);
    }

    public function has(string|Reference $reference): bool
    {
        $reference = UnresolvedReference::init($reference);

        foreach ($this->refs as $ref) {
            if ($ref->is($reference)) {
                return true;
            }
        }

        return false;
    }

    /**
     * @return list<string> the columns the references read: an alias names an output, never the column read
     */
    public function names(): array
    {
        $names = [];

        foreach ($this->refs as $ref) {
            $names[] = $ref->to();
        }

        return $names;
    }

    /**
     * @param string $offset
     *
     * @return bool
     */
    public function offsetExists($offset): bool
    {
        return array_key_exists($offset, $this->refs);
    }

    /**
     * @param string $offset
     *
     * @throws InvalidArgumentException
     *
     * @return Reference
     */
    public function offsetGet($offset): Reference
    {
        if ($this->offsetExists($offset)) {
            return $this->refs[$offset];
        }

        throw new InvalidArgumentException("Reference {$offset} does not exists.");
    }

    public function offsetSet(mixed $offset, mixed $value): void
    {
        throw new InvalidArgumentException('Method not implemented.');
    }

    public function offsetUnset(mixed $offset): void
    {
        throw new InvalidArgumentException('Method not implemented.');
    }

    public function reverse(): self
    {
        return new self(...array_reverse($this->refs));
    }

    public function without(string|Reference ...$reference): self
    {
        foreach ($reference as $ref) {
            $refName = $ref instanceof Reference ? $ref->name() : $ref;

            $this->without[$refName] = true;

            if (array_key_exists($refName, $this->refs)) {
                unset($this->refs[$refName]);
            }
        }

        return $this;
    }
}
