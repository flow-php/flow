<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\CreateView;

interface CreateViewCheckOptionStep extends CreateViewFinalStep
{
    public function withCascadedCheckOption() : CreateViewFinalStep;

    public function withCheckOption() : CreateViewFinalStep;

    public function withLocalCheckOption() : CreateViewFinalStep;
}
