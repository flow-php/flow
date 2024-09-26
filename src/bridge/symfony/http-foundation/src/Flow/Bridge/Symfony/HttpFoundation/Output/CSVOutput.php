<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Output;

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\Filesystem\DSL\path_stdout;
use Flow\Bridge\Symfony\HttpFoundation\Output;
use Flow\ETL\Loader;

if (!function_exists('Flow\ETL\Adapter\CSV\to_csv')) {
    throw new \RuntimeException('Flow\ETL\Adapter\CSV\to_csv function is not available. Make sure that composer require flow-php/etl-adapter-json dependency is present in your composer.json.');
}

final class CSVOutput implements Output
{
    public function __construct(
        private readonly bool $withHeader = true,
        private readonly string $separator = ',',
        private readonly string $enclosure = '"',
        private readonly string $escape = '\\',
        private readonly string $newLineSeparator = PHP_EOL,
        private readonly string $datetimeFormat = \DateTimeInterface::ATOM,
    ) {

    }

    public function loader() : Loader
    {
        return to_csv(path_stdout(['stream' => 'output']))
            ->withHeader($this->withHeader)
            ->withSeparator($this->separator)
            ->withEnclosure($this->enclosure)
            ->withEscape($this->escape)
            ->withNewLineSeparator($this->newLineSeparator)
            ->withDateTimeFormat($this->datetimeFormat);
    }

    public function type() : Type
    {
        return Type::CSV;
    }
}
