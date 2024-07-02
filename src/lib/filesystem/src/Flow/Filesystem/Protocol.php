<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Flow\Filesystem\Exception\InvalidSchemeException;

final class Protocol
{
    public function __construct(public readonly string $name)
    {
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
