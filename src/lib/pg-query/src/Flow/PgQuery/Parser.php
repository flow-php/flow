<?php

declare(strict_types=1);

namespace Flow\PgQuery;

use Flow\PgQuery\Exception\{ExtensionNotLoadedException, ParserException};
use Flow\PgQuery\Protobuf\AST\ParseResult;

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
        $result = pg_query_fingerprint($sql);

        return $result === false ? null : $result;
    }

    public function normalize(string $sql) : ?string
    {
        $result = pg_query_normalize((new NamedParameterNormalizer())->normalize($sql));

        return $result === false ? null : $result;
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
}
