<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Schema\Catalog;

interface ViewDependencyResolver
{
    /**
     * @param list<string> $modifiedTableQualifiedNames
     */
    public function resolve(Catalog $catalog, array $modifiedTableQualifiedNames): DependentViews;
}
