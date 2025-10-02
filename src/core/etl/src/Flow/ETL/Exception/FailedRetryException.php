<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Retry\RetriesRecord;

final class FailedRetryException extends RuntimeException
{
    public function __construct(
        public readonly RetriesRecord $record,
        string $message = '',
    ) {
        if ($message === '') {
            $totalAttempts = $record->count();

            $message = \sprintf('Retry failed after %d attempts.', $totalAttempts);
        }

        parent::__construct($message, 0, $record->last()?->exception);
    }
}
