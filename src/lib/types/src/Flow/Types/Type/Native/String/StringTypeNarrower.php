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

final class StringTypeNarrower implements TypeNarrower
{
    private string $string;

    /**
     * @return Type<mixed>
     */
    public function narrow(mixed $value) : Type
    {
        if (!\is_string($value)) {
            return type_string();
        }

        $this->string = \trim($value);

        if ($this->isNull()) {
            return type_null();
        }

        if ($this->isJson()) {
            return type_json();
        }

        if ($this->isUuid()) {
            return type_uuid();
        }

        if ($this->isHTML()) {
            return type_html();
        }

        if ($this->isXML()) {
            return type_xml();
        }

        if ($this->isDateTime()) {
            return type_datetime();
        }

        if ($this->isDate()) {
            return type_date();
        }

        if ($this->isBoolean()) {
            return type_boolean();
        }

        if ($this->isFloat()) {
            return type_float();
        }

        if ($this->isInteger()) {
            return type_integer();
        }

        if ($this->isTimeZone()) {
            return type_time_zone();
        }

        return type_string();
    }

    private function isBoolean() : bool
    {
        if ($this->string === '') {
            return false;
        }

        return \in_array(\strtolower($this->string), ['true', 'false'], true);
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

        if (($dateParts['hour'] ?? false) !== false) {
            return false;
        }

        if (($dateParts['minute'] ?? false) !== false) {
            return false;
        }

        if (($dateParts['second'] ?? false) !== false) {
            return false;
        }

        if (($dateParts['fraction'] ?? false) !== false) {
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

        $hasDirectTime = ($dateParts['hour'] ?? false) !== false
            || ($dateParts['minute'] ?? false) !== false
            || ($dateParts['second'] ?? false) !== false
            || ($dateParts['fraction'] ?? false) !== false;

        $hasRelativeTime = false;

        if (isset($dateParts['relative']) && \is_array($dateParts['relative'])) {
            $relative = $dateParts['relative'];
            $hasRelativeTime = ($relative['hour'] ?? 0) !== 0 || ($relative['minute'] ?? 0) !== 0 || ($relative['second'] ?? 0) !== 0;
        }

        return $hasDirectTime || $hasRelativeTime;
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
        return type_json()->isValid($this->string);
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
        return Uuid::isValid($this->string);
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
