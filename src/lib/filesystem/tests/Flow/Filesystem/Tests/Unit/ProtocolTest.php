<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Filesystem\Protocol;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class ProtocolTest extends TestCase
{
    #[TestWith(['azure_blob'])]
    #[TestWith([''])]
    #[TestWith(['azure-blob://'])]
    public function test_protocol_name_must_be_valid(string $scheme) : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Invalid protocol name: '{$scheme}'. Only alphanumeric characters, dots, hyphens and plus signs are allowed.");

        new Protocol($scheme);
    }
}
