<?php

declare(strict_types=1);

namespace Flow\Floe;

use Closure;
use Flow\ETL\Row\Entry;
use Flow\ETL\Schema\Definition;
use ReflectionClass;

final class EntryFactory
{
    /**
     * @var \Closure(string, mixed, Definition<mixed>): Entry<mixed>
     */
    private readonly Closure $instantiate;

    /**
     * @param \Closure(string, mixed, Definition<mixed>): Entry<mixed> $instantiate
     */
    private function __construct(Closure $instantiate)
    {
        $this->instantiate = $instantiate;
    }

    /**
     * @param class-string<Entry<mixed>> $entryClass
     */
    public static function forEntryClass(string $entryClass): self
    {
        $reflection = new ReflectionClass($entryClass);

        /** @var \Closure(string, mixed, Definition<mixed>): Entry<mixed> $instantiate */
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
     * @return Entry<mixed>
     */
    public function create(string $name, mixed $value, Definition $definition): Entry
    {
        return ($this->instantiate)($name, $value, $definition);
    }
}
