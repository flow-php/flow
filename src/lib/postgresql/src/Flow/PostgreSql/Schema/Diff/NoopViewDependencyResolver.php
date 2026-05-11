<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Schema\Catalog;

final readonly class NoopViewDependencyResolver implements ViewDependencyResolver
{
    public function resolve(Catalog $catalog, array $modifiedTableQualifiedNames): DependentViews
    {
        return DependentViews::empty();
    }
}
