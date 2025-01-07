<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Output;

use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\Filesystem\DSL\path_stdout;
use Flow\Bridge\Symfony\HttpFoundation\Output;
use Flow\ETL\Loader;

if (!function_exists('Flow\ETL\Adapter\JSON\to_json')) {
    throw new \RuntimeException('Flow\ETL\Adapter\JSON\to_json function is not available. Make sure that composer require flow-php/etl-adapter-json dependency is present in your composer.json.');
}

final readonly class JsonOutput implements Output
{
    public function __construct(
        private int $flags = JSON_THROW_ON_ERROR,
        private string $dateTimeFormat = \DateTimeInterface::ATOM,
        private bool $putRowsInNewLines = false,
    ) {

    }

    public function loader() : Loader
    {
        return to_json(path_stdout(['stream' => 'output']))
            ->withFlags($this->flags)
            ->withDateTimeFormat($this->dateTimeFormat)
            ->withRowsInNewLines($this->putRowsInNewLines);
    }

    public function type() : Type
    {
        return Type::JSON;
    }
}
