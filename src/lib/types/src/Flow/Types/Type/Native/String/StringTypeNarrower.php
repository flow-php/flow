<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native\String;

use function Flow\Types\DSL\{type_boolean,
    type_date,
    type_datetime,
    type_float,
    type_html,
    type_integer,
    type_json,
    type_null,
    type_string,
    type_time_zone,
    type_uuid,
    type_xml};
use Flow\Types\Type;
use Flow\Types\Type\TypeNarrower;
use Flow\Types\Value\{HTMLDocument, Uuid};

final readonly class StringTypeNarrower implements TypeNarrower
{
    private string $string;

    private function __construct(string $string)
    {
        $this->string = \trim($string);
    }

    /**
     * @return Type<mixed>
     */
    public static function narrow(mixed $value) : Type
    {
        if (!\is_string($value)) {
            return type_string();
        }

        $checker = new self($value);

        if ($checker->isNull()) {
            return type_null();
        }

        if ($checker->isJson()) {
            return type_json();
        }

        if ($checker->isUuid()) {
            return type_uuid();
        }

        if ($checker->isHTML()) {
            return type_html();
        }

        if ($checker->isXML()) {
            return type_xml();
        }

        if ($checker->isBoolean()) {
            return type_boolean();
        }

        if ($checker->isFloat()) {
            return type_float();
        }

        if ($checker->isInteger()) {
            return type_integer();
        }

        if ($checker->isDate()) {
            return type_date();
        }

        if ($checker->isDateTime()) {
            return type_datetime();
        }

        if ($checker->isTimeZone()) {
            return type_time_zone();
        }

        return type_string();
    }

    private function isBoolean() : bool
    {
        if ($this->string === '') {
            return false;
        }

        return \in_array(\strtolower($this->string), ['true', 'false', 'yes', 'no', 'on', 'off'], true);
    }

    private function isDate() : bool
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

        if ($dateParts['hour'] !== false) {
            return false;
        }

        if ($dateParts['minute'] !== false) {
            return false;
        }

        if ($dateParts['second'] !== false) {
            return false;
        }

        if ($dateParts['fraction'] !== false) {
            return false;
        }

        return true;
    }

    private function isDateTime() : bool
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

    private function isFloat() : bool
    {
        if ($this->string === '') {
            return false;
        }

        // scientific notation
        if (\is_numeric($this->string) && (\str_contains($this->string, 'e') || \str_contains($this->string, 'E'))) {
            return true;
        }

        return \is_numeric($this->string) && \str_contains($this->string, '.');
    }

    private function isHTML() : bool
    {
        return HTMLDocument::isValid($this->string);
    }

    private function isInteger() : bool
    {
        if ($this->string === '') {
            return false;
        }

        if (\is_numeric($this->string)) {
            return (string) ((int) $this->string) === $this->string;
        }

        return false;
    }

    private function isJson() : bool
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

    private function isNull() : bool
    {
        return \in_array(\mb_strtolower($this->string), ['null', 'nil'], true);
    }

    private function isTimeZone() : bool
    {
        if ($this->string === '') {
            return false;
        }

        if (\in_array($this->string, \DateTimeZone::listIdentifiers(), true)) {
            return true;
        }

        if (\preg_match('/^[+-]\d{2}:\d{2}$/', $this->string) === 1) {
            try {
                new \DateTimeZone($this->string);

                return true;
            } catch (\Exception) {
                return false;
            }
        }

        return false;
    }

    private function isUuid() : bool
    {
        if ($this->string === '') {
            return false;
        }

        if (\strlen($this->string) !== 36) {
            return false;
        }

        return 0 !== \preg_match(Uuid::UUID_REGEXP, $this->string);
    }

    private function isXML() : bool
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
                $result = @$doc->loadXML($this->string);

                return (bool) $result;
            } catch (\Exception) {
                return false;
            } finally {
                \libxml_clear_errors(); // Clear any errors if needed
                \libxml_use_internal_errors(false); // Restore standard error handling
            }
        }

        return false;
    }
}
