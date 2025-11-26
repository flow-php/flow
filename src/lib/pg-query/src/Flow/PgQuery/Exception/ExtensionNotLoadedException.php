<?php

declare(strict_types=1);

namespace Flow\PgQuery\Exception;

final class ExtensionNotLoadedException extends \RuntimeException
{
    public function __construct()
    {
        parent::__construct('pg_query extension is not loaded. Install flow-php/pg-query-ext via PIE.');
    }
}
