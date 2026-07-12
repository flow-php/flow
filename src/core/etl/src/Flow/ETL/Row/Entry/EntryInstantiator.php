<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Closure;
use Flow\ETL\Row\Entry;
use Flow\ETL\Schema\Definition;
use ReflectionClass;

/**
 * @template-covariant T of Entry<mixed>
 */
final readonly class EntryInstantiator
{
    /**
     * @var \Closure(string, mixed, Definition<mixed>): T
     */
    private Closure $instantiate;

    /**
     * @param \Closure(string, mixed, Definition<mixed>): T $instantiate
     */
    private function __construct(Closure $instantiate)
    {
        $this->instantiate = $instantiate;
    }

    /**
     * @template TEntry of Entry<mixed>
     *
     * @param class-string<TEntry> $entryClass
     *
     * @return self<TEntry>
     */
    public static function forClass(string $entryClass): self
    {
        $reflection = new ReflectionClass($entryClass);

        /** @var \Closure(string, mixed, Definition<mixed>): TEntry $instantiate */
        $instantiate = Closure::bind(
            static function (string $name, mixed $value, Definition $definition) use ($reflection): Entry {
                $entry = $reflection->newInstanceWithoutConstructor();
                // @mago-ignore analysis:non-existent-property
                $entry->name = $name;
                // @mago-ignore analysis:non-existent-property
                $entry->value = $value;
                // @mago-ignore analysis:non-existent-property
                $entry->definition = $definition;

                return $entry;
            },
            null,
            $entryClass,
        );

        return new self($instantiate);
    }

    /**
     * @param Definition<mixed> $definition
     *
     * @return T
     */
    public function instantiate(string $name, mixed $value, Definition $definition): Entry
    {
        return ($this->instantiate)($name, $value, $definition);
    }
}
