<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native\String;

use DateTimeZone;
use DOMDocument;
use Exception;
use Flow\Types\Type;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\TypeNarrower;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;

use function array_fill_keys;
use function array_key_exists;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function in_array;
use function is_numeric;
use function is_string;
use function libxml_clear_errors;
use function libxml_use_internal_errors;
use function mb_strtolower;
use function preg_match;
use function str_contains;
use function strtolower;
use function trim;

final class StringTypeNarrower implements TypeNarrower
{
    /**
     * DateTimeZone::listIdentifiers() is 419 entries and was rebuilt on every cell that reached the rung.
     * Static because AutoCaster::castToString() constructs a narrower per value.
     *
     * @var null|array<string, true>
     */
    private static ?array $timeZoneIdentifiers = null;

    /**
     * @var null|array<class-string<Type<mixed>>, true>
     */
    private readonly ?array $emits;

    /**
     * @param null|list<Type<mixed>> $emits - the types this narrower may return; null runs every rung.
     *                                        A rung whose type is not listed never evaluates its predicate.
     */
    public function __construct(?array $emits = null)
    {
        if ($emits === null) {
            $this->emits = null;

            return;
        }

        $classes = [];

        foreach ($emits as $type) {
            $classes[$type::class] = true;
        }

        $this->emits = $classes;
    }

    /**
     * @param Type<mixed> $type
     */
    public function emitsType(Type $type): bool
    {
        return $this->emits === null || array_key_exists($type::class, $this->emits);
    }

    /**
     * @return Type<mixed>
     */
    public function narrow(mixed $value): Type
    {
        if (!is_string($value)) {
            return type_string();
        }

        $value = trim($value);

        if ($value === '') {
            return type_string();
        }

        // a rung that cannot produce a candidate is skipped, so its predicate never runs - that is where
        // all-strings mode gets its speed, and why isXML() no longer builds a DOMDocument per cell
        $temporal = $this->emitsType(type_datetime()) || $this->emitsType(type_date())
            ? StringTemporalParts::from($value)
            : new StringTemporalParts(false, false, false);

        return match (true) {
            $this->isNull($value) => type_null(),
            $this->emitsType(type_json()) && $this->isJson($value) => type_json(),
            $this->emitsType(type_uuid()) && $this->isUuid($value) => type_uuid(),
            $this->emitsType(type_html()) && $this->isHTML($value) => type_html(),
            $this->emitsType(type_xml()) && $this->isXML($value) => type_xml(),
            $this->emitsType(type_float()) && $this->isFloat($value) => type_float(),
            $this->emitsType(type_integer()) && $this->isInteger($value) => type_integer(),
            $this->emitsType(type_datetime()) && $temporal->isDateTime() => type_datetime(),
            $this->emitsType(type_date()) && $temporal->isDate() => type_date(),
            $this->emitsType(type_boolean()) && $this->isBoolean($value) => type_boolean(),
            $this->emitsType(type_time_zone()) && $this->isTimeZone($value) => type_time_zone(),
            default => type_string(),
        };
    }

    /**
     * @param non-empty-string $value
     */
    private function isBoolean(string $value): bool
    {
        return in_array(strtolower($value), ['true', 'false'], true);
    }

    /**
     * @param non-empty-string $value
     */
    private function isFloat(string $value): bool
    {
        if (!is_numeric($value)) {
            return false;
        }

        // scientific notation
        if (str_contains($value, 'e') || str_contains($value, 'E')) {
            return true;
        }

        return str_contains($value, '.');
    }

    /**
     * @param non-empty-string $value
     */
    private function isHTML(string $value): bool
    {
        if ('<' !== $value[0]) {
            return false;
        }

        return preg_match(HTMLType::HTML_ALIKE_REGEX, $value) === 1;
    }

    /**
     * @param non-empty-string $value
     */
    private function isInteger(string $value): bool
    {
        if (is_numeric($value)) {
            return (string) (int) $value === $value;
        }

        return false;
    }

    /**
     * @param non-empty-string $value
     */
    private function isJson(string $value): bool
    {
        return Json::isValid($value);
    }

    /**
     * @param non-empty-string $value
     */
    private function isNull(string $value): bool
    {
        return in_array(mb_strtolower($value), ['null', 'nil'], true);
    }

    /**
     * @param non-empty-string $value
     */
    private function isTimeZone(string $value): bool
    {
        self::$timeZoneIdentifiers ??= array_fill_keys(DateTimeZone::listIdentifiers(), true);

        if (array_key_exists($value, self::$timeZoneIdentifiers)) {
            return true;
        }

        if (preg_match('/^[+-]\d{2}:\d{2}$/', $value) === 1) {
            try {
                new DateTimeZone($value);

                return true;
            } catch (Exception) {
                return false;
            }
        }

        return false;
    }

    /**
     * @param non-empty-string $value
     */
    private function isUuid(string $value): bool
    {
        return Uuid::isValid($value);
    }

    /**
     * @param non-empty-string $value
     */
    private function isXML(string $value): bool
    {
        if ('<' !== $value[0]) {
            return false;
        }

        if (preg_match('/<(.+?)>(.+?)<\/(.+?)>/', $value) === 1) {
            try {
                libxml_use_internal_errors(true);

                $doc = new DOMDocument();

                return @$doc->loadXML($value);
            } catch (Exception) {
                return false;
            } finally {
                libxml_clear_errors(); // Clear any errors if needed
                libxml_use_internal_errors(false); // Restore standard error handling
            }
        }

        return false;
    }
}
