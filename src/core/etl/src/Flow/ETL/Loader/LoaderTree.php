<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Loader;
use SplObjectStorage;

use function array_shift;

final readonly class LoaderTree
{
    /**
     * @return list<Loader>
     */
    public function flatten(Loader $root): array
    {
        /** @var SplObjectStorage<Loader, null> $visited */
        $visited = new SplObjectStorage();
        $queue = [$root];
        $flattened = [];

        while ($queue !== []) {
            $loader = array_shift($queue);

            if ($visited->contains($loader)) {
                continue;
            }

            $visited->attach($loader);
            $flattened[] = $loader;

            if ($loader instanceof OverridingLoader) {
                foreach ($loader->loaders() as $overridden) {
                    $queue[] = $overridden;
                }
            }
        }

        return $flattened;
    }
}
