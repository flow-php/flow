<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

final class ContextException extends ClientException
{
    public static function keyNotFound(string $key): self
    {
        return new self(\sprintf('Context has no value for key "%s".', $key));
    }
}
