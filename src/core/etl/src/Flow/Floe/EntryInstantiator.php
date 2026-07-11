<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row\Entry;

use function array_key_exists;

final class EntryInstantiator
{
    /**
     * @var array<class-string, EntryFactory>
     */
    private array $factories = [];

    /**
     * @param class-string<Entry<mixed>> $entryClass
     */
    public function factoryFor(string $entryClass): EntryFactory
    {
        if (array_key_exists($entryClass, $this->factories)) {
            return $this->factories[$entryClass];
        }

        return $this->factories[$entryClass] = EntryFactory::forEntryClass($entryClass);
    }
}
