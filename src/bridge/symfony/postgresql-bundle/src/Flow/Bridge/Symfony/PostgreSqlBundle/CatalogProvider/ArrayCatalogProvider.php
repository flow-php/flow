<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\CatalogProvider;

use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\CatalogProvider;

/**
 * @import-type CatalogShape from Catalog
 */
final readonly class ArrayCatalogProvider implements CatalogProvider
{
    /**
     * @param CatalogShape $data
     */
    public function __construct(
        private array $data,
    ) {}

    public function get(): Catalog
    {
        return Catalog::fromArray($this->data);
    }
}
