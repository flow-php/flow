<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

final readonly class ManualCatalogProvider implements CatalogProvider
{
    public function __construct(
        private Catalog $catalog,
    ) {
    }

    public function get() : Catalog
    {
        return $this->catalog;
    }
}
