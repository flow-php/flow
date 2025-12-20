<?php

declare(strict_types=1);

namespace Flow\PostgreSql;

use Flow\PostgreSql\Exception\{ExtensionNotLoadedException, ParserException};
use Flow\PostgreSql\Protobuf\AST\ParseResult;

final class Parser
{
    public function __construct()
    {
        if (!extension_loaded('pg_query')) {
            throw new ExtensionNotLoadedException();
        }
    }

    public function fingerprint(string $sql) : ?string
    {
        return pg_query_fingerprint($sql) ?: null;
    }

    public function normalize(string $sql) : ?string
    {
        return pg_query_normalize((new NamedParameterNormalizer())->normalize($sql)) ?: null;
    }

    public function normalizeUtility(string $sql) : ?string
    {
        return pg_query_normalize_utility($sql) ?: null;
    }

    public function parse(string $sql) : ParsedQuery
    {
        try {
            $json = pg_query_parse($sql);
        } catch (\RuntimeException $e) {
            throw new ParserException($e->getMessage());
        }

        $result = new ParseResult();
        $result->mergeFromJsonString($json);

        return new ParsedQuery($result);
    }

    /**
     * @return array<string>
     */
    public function split(string $sql) : array
    {
        return pg_query_split($sql);
    }

    public function summary(string $sql, int $options = 0, int $truncateLimit = 0) : string
    {
        try {
            return pg_query_summary($sql, $options, $truncateLimit);
        } catch (\RuntimeException $e) {
            throw new ParserException($e->getMessage());
        }
    }
}
