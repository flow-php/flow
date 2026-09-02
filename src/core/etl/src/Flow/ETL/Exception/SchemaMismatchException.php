<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use function sprintf;

/**
 * A row's own violation, placed in the batch that holds it - the row cannot report its position,
 * only the batch knows it.
 */
final class SchemaMismatchException extends InvalidArgumentException
{
    public function __construct(int $rowIndex, ColumnMismatchException $cause)
    {
        parent::__construct(
            sprintf(
                'Rows do not match their schema: column "%s" (row %d)%s',
                $cause->column,
                $rowIndex,
                $cause->detail,
            ),
            0,
            $cause,
        );
    }
}
