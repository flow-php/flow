<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\CreateTable;

/**
 * Interface for building temporary tables with both column definitions and ON COMMIT options.
 *
 * This interface combines CreateTableColumnsStep for adding columns and
 * CreateTableTemporaryStep for specifying ON COMMIT behavior.
 */
interface CreateTemporaryTableColumnsStep extends CreateTableColumnsStep, CreateTableTemporaryStep
{
}
