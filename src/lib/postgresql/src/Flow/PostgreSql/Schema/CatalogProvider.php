<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

interface CatalogProvider
{
    public function get(): Catalog;
}
