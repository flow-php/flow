<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

final readonly class ChainCatalogProvider implements CatalogProvider
{
    /**
     * @var list<CatalogProvider>
     */
    private array $providers;

    public function __construct(CatalogProvider ...$providers)
    {
        $this->providers = \array_values($providers);
    }

    public function get() : Catalog
    {
        $catalog = new Catalog([]);

        foreach ($this->providers as $provider) {
            $catalog = $catalog->merge($provider->get());
        }

        return $catalog;
    }
}
