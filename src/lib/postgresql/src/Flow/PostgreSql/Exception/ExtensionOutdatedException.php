<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Exception;

use RuntimeException;

final class ExtensionOutdatedException extends RuntimeException
{
    public function __construct()
    {
        parent::__construct(
            'The loaded pg_query extension does not keep comments in deparse. Update flow-php/pg-query-ext to 0.49 or later, or remove the DeparseOptions::preserveComments() call.',
        );
    }
}
