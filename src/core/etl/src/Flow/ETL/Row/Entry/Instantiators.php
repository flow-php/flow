<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Row\Entry;

use function array_key_exists;

final class Instantiators
{
    /**
     * @var array<class-string<Entry<mixed>>, EntryInstantiator<Entry<mixed>>>
     */
    private array $instantiators = [];

    /**
     * @template T of Entry<mixed>
     *
     * @param class-string<T> $entryClass
     *
     * @return EntryInstantiator<T>
     */
    public function for(string $entryClass): EntryInstantiator
    {
        if (!array_key_exists($entryClass, $this->instantiators)) {
            $this->instantiators[$entryClass] = EntryInstantiator::forClass($entryClass);
        }

        /** @var EntryInstantiator<T> */
        return $this->instantiators[$entryClass];
    }
}
