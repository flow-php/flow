<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Rows;
use Flow\ETL\Schema;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class NativeCSVWriter
{
    public function __construct(
        string $separator,
        string $enclosure,
        string $escape,
        string $newLineSeparator,
        string $dateTimeFormat,
        string $dateFormat,
    ) {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param array<string, list<?string>> $cells CSVEncoder::cells() of the unrendered columns, each of $rows->count() values
     */
    public function encode(Rows $rows, array $cells): string
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
