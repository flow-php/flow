<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Factory;

use Flow\ETL\Row\Entry\Type\Uuid;

final class StringTypeChecker
{
    private readonly string $string;

    public function __construct(string $string)
    {
        $this->string = \trim($string);
    }

    public function isBoolean() : bool
    {
        if ($this->string === '') {
            return false;
        }

        return \in_array(\strtolower($this->string), ['true', 'false'], true);
    }

    public function isDateTime() : bool
    {
        if ($this->string === '') {
            return false;
        }

        $dateParts = \date_parse($this->string);

        if ($dateParts['error_count'] > 0) {
            return false;
        }

        if ($dateParts['year'] === false) {
            return false;
        }

        if ($dateParts['month'] === false) {
            return false;
        }

        if ($dateParts['day'] === false) {
            return false;
        }

        return true;
    }

    public function isFloat() : bool
    {
        if ($this->string === '') {
            return false;
        }

        return \is_numeric($this->string) && \str_contains($this->string, '.');
    }

    public function isInteger() : bool
    {
        if ($this->string === '') {
            return false;
        }

        if (\is_numeric($this->string)) {
            return (string) ((int) $this->string) === $this->string;
        }

        return false;
    }

    public function isJson() : bool
    {
        if ($this->string === '') {
            return false;
        }

        if ('{' !== $this->string[0] && '[' !== $this->string[0]) {
            return false;
        }

        if (\function_exists('json_validate')) {
            return \json_validate($this->string);
        }

        if (
            (!\str_starts_with($this->string, '{') || !\str_ends_with($this->string, '}'))
            && (!\str_starts_with($this->string, '[') || !\str_ends_with($this->string, ']'))
        ) {
            return false;
        }

        try {
            return \is_array(\json_decode($this->string, true, flags: \JSON_THROW_ON_ERROR));
        } catch (\Exception) {
            return false;
        }
    }

    public function isNull() : bool
    {
        return \in_array(\mb_strtolower($this->string), ['null', 'nil'], true);
    }

    public function isUuid() : bool
    {
        if ($this->string === '') {
            return false;
        }

        if (\strlen($this->string) !== 36) {
            return false;
        }

        return 0 !== \preg_match(Uuid::UUID_REGEXP, $this->string);
    }

    public function isXML() : bool
    {
        if ($this->string === '') {
            return false;
        }

        if ('<' !== $this->string[0]) {
            return false;
        }

        if (\preg_match('/<(.+?)>(.+?)<\/(.+?)>/', $this->string) === 1) {
            try {
                \libxml_use_internal_errors(true);

                $doc = new \DOMDocument();
                $result = $doc->loadXML($this->string);
                \libxml_clear_errors(); // Clear any errors if needed
                \libxml_use_internal_errors(false); // Restore standard error handling

                /** @psalm-suppress RedundantCastGivenDocblockType */
                return (bool) $result;
            } catch (\Exception) {
                \libxml_clear_errors(); // Clear any errors if needed
                \libxml_use_internal_errors(false); // Restore standard error handling

                return false;
            }
        }

        return false;
    }

    public function value() : string
    {
        return $this->string;
    }
}
