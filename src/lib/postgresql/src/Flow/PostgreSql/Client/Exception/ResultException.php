<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

final class ResultException extends ClientException
{
    public static function sequenceNotUsed(string $sequenceName) : self
    {
        return new self(\sprintf('Sequence "%s" has not been used in this session', $sequenceName));
    }

    public static function unexpectedScalarType(string $expected, string $actual) : self
    {
        return new self(\sprintf('Expected scalar of type %s, got %s', $expected, $actual));
    }
}
