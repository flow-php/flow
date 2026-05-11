<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Schema\MaterializedView;
use Flow\PostgreSql\Schema\View;

final readonly class DependentView
{
    public function __construct(
        public string $schema,
        public View|MaterializedView $view,
    ) {}

    public function qualifiedName(): string
    {
        return $this->schema . '.' . $this->view->name;
    }
}
