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
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\TypeNarrower;
use Flow\Types\Value\Uuid;

final class StringTypeNarrower implements TypeNarrower
{
    /**
     * @return Type<mixed>
     */
    #[\Override]
    public function narrow(mixed $value) : Type
    {
        if (!\is_string($value)) {
            return type_string();
        }

        $value = \trim($value);

        if ($value === '') {
            return type_string();
        }

        return match (true) {
            $this->isNull($value) => type_null(),
            $this->isJson($value) => type_json(),
            $this->isUuid($value) => type_uuid(),
            $this->isHTML($value) => type_html(),
            $this->isXML($value) => type_xml(),
            $this->isDateTime($value) => type_datetime(),
            $this->isDate($value) => type_date(),
            $this->isBoolean($value) => type_boolean(),
            $this->isFloat($value) => type_float(),
            $this->isInteger($value) => type_integer(),
            $this->isTimeZone($value) => type_time_zone(),
            default => type_string(),
        };
    }

    /**
     * @param non-empty-string $value
     */
    private function isBoolean(string $value) : bool
    {
        return \in_array(\strtolower($value), ['true', 'false'], true);
    }

    /**
     * @param non-empty-string $value
     */
    private function isDate(string $value) : bool
    {
        $dateParts = \date_parse($value);

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

    /**
     * @param non-empty-string $value
     */
    private function isDateTime(string $value) : bool
    {
        $dateParts = \date_parse($value);

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

        if ($hasDirectTime) {
            return true;
        }

        if (\is_array($dateParts['relative'] ?? false)) {
            $relative = $dateParts['relative'];

            return ($relative['hour'] ?? 0) !== 0 || ($relative['minute'] ?? 0) !== 0 || ($relative['second'] ?? 0) !== 0;
        }

        return false;
    }

    /**
     * @param non-empty-string $value
     */
    private function isFloat(string $value) : bool
    {
        if (!\is_numeric($value)) {
            return false;
        }

        // scientific notation
        if (\str_contains($value, 'e') || \str_contains($value, 'E')) {
            return true;
        }

        return \str_contains($value, '.');
    }

    /**
     * @param non-empty-string $value
     */
    private function isHTML(string $value) : bool
    {
        if ('<' !== $value[0]) {
            return false;
        }

        return \preg_match(HTMLType::HTML_ALIKE_REGEX, $value) === 1;
    }

    /**
     * @param non-empty-string $value
     */
    private function isInteger(string $value) : bool
    {
        if (\is_numeric($value)) {
            return (string) ((int) $value) === $value;
        }

        return false;
    }

    /**
     * @param non-empty-string $value
     */
    private function isJson(string $value) : bool
    {
        return type_json()->isValid($value);
    }

    /**
     * @param non-empty-string $value
     */
    private function isNull(string $value) : bool
    {
        return \in_array(\mb_strtolower($value), ['null', 'nil'], true);
    }

    /**
     * @param non-empty-string $value
     */
    private function isTimeZone(string $value) : bool
    {
        if (\in_array($value, \DateTimeZone::listIdentifiers(), true)) {
            return true;
        }

        if (\preg_match('/^[+-]\d{2}:\d{2}$/', $value) === 1) {
            try {
                new \DateTimeZone($value);

                return true;
            } catch (\Exception) {
                return false;
            }
        }

        return false;
    }

    /**
     * @param non-empty-string $value
     */
    private function isUuid(string $value) : bool
    {
        return Uuid::isValid($value);
    }

    /**
     * @param non-empty-string $value
     */
    private function isXML(string $value) : bool
    {
        if ('<' !== $value[0]) {
            return false;
        }

        if (\preg_match('/<(.+?)>(.+?)<\/(.+?)>/', $value) === 1) {
            try {
                \libxml_use_internal_errors(true);

                $doc = new \DOMDocument();
                $result = @$doc->loadXML($value);

                return  $result;
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
