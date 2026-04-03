<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Double;

use Flow\PostgreSql\Schema\{Catalog, CatalogProvider};

final class FakeCatalogProvider implements CatalogProvider
{
    public function __construct(
        public Catalog $catalog,
    ) {
    }

    public function get() : Catalog
    {
        return $this->catalog;
    }
}
