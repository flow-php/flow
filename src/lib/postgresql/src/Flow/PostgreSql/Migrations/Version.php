<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

use Flow\PostgreSql\Migrations\Exception\MigrationException;

final readonly class Version implements \Stringable
{
    private function __construct(
        private string $version,
    ) {
    }

    public static function fromString(string $version) : self
    {
        if (\preg_match('/^[a-zA-Z0-9_]+$/', $version) !== 1 || \strlen($version) > 255) {
            throw MigrationException::invalidVersionFormat($version);
        }

        return new self($version);
    }

    public function __toString() : string
    {
        return $this->version;
    }

    public function equals(self $other) : bool
    {
        return $this->version === $other->version;
    }

    public function isAfter(self $other) : bool
    {
        return \strcmp($this->version, $other->version) > 0;
    }
}
