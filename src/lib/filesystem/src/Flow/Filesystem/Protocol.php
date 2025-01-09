<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\InvalidSchemeException;

final readonly class Protocol
{
    public function __construct(public string $name)
    {
        if (!\preg_match('/^[a-zA-Z][a-zA-Z0-9+.-]+$/', $name)) {
            throw new InvalidArgumentException("Invalid protocol name: '{$name}'. Only alphanumeric characters, dots, hyphens and plus signs are allowed.");
        }
    }

    public function is(string $name) : bool
    {
        return \mb_strtolower(\str_replace('://', '', $this->name)) === \str_replace('://', '', \mb_strtolower($name));
    }

    public function scheme() : string
    {
        return $this->name . '://';
    }

    public function validateScheme(string|Path $scheme) : void
    {
        if ($scheme instanceof Path) {
            $scheme = $scheme->protocol()->scheme();
        }

        if (!$this->is($scheme)) {
            throw new InvalidSchemeException($scheme, $this->scheme());
        }
    }
}
