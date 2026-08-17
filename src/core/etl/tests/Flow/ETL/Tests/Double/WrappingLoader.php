<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\OverridingLoader;
use Flow\ETL\Rows;

final class WrappingLoader implements Closure, Loader, OverridingLoader
{
    /**
     * @var array<Loader>
     */
    public array $wrapped;

    public function __construct(Loader ...$wrapped)
    {
        $this->wrapped = $wrapped;
    }

    public function closure(FlowContext $context): void
    {
        foreach ($this->wrapped as $loader) {
            if ($loader instanceof Closure) {
                $loader->closure($context);
            }
        }
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        foreach ($this->wrapped as $loader) {
            $loader->load($rows, $context);
        }
    }

    public function loaders(): array
    {
        return $this->wrapped;
    }
}
