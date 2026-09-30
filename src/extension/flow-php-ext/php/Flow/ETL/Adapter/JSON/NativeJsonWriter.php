<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class NativeJsonWriter
{
    /**
     * @throws InvalidArgumentException a flag outside JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION
     */
    public function __construct(int $flags, string $dateTimeFormat, string $dateFormat)
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param array<string, list<string>> $fragments JSONEncoder::fragments() of the unrendered columns
     */
    public function encode(Rows $rows, array $fragments, string $separator): string
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @return list<string> the columns of $schema this writer does not render itself
     */
    public function unrendered(Schema $schema): array
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
