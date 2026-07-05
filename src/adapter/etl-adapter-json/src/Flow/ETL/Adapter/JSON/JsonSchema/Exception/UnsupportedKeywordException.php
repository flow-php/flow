<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JsonSchema\Exception;

use Flow\ETL\Exception\RuntimeException;

use function sprintf;

final class UnsupportedKeywordException extends RuntimeException
{
    public function __construct(string $keyword, string $path)
    {
        parent::__construct(sprintf(
            'JSON Schema keyword "%s" at path "%s" is not supported by the Flow schema converter',
            $keyword,
            $path,
        ));
    }
}
