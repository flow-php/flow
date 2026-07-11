<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\NonEmptyStringType;
use Flow\Types\Type\Logical\NumericStringType;
use Flow\Types\Type\Logical\PositiveIntegerType;
use Flow\Types\Type\Logical\ScalarType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use WeakMap;

use function array_key_exists;
use function json_encode;

/**
 * Decides whether a row fits the current section plan or a new SCHEMA frame must
 * be emitted.
 */
final class SchemaTracker
{
    private const CONSTANT_NORMALIZE_TYPES = [
        IntegerType::class => true,
        PositiveIntegerType::class => true,
        FloatType::class => true,
        BooleanType::class => true,
        StringType::class => true,
        NonEmptyStringType::class => true,
        NumericStringType::class => true,
        ScalarType::class => true,
        DateTimeType::class => true,
        DateType::class => true,
        TimeType::class => true,
        HTMLType::class => true,
        HTMLElementType::class => true,
        TimeZoneType::class => true,
        UuidType::class => true,
        JsonType::class => true,
        XMLType::class => true,
        XMLElementType::class => true,
    ];

    /**
     * @var array<class-string, string>
     */
    private array $fingerprints = [];

    /**
     * Container types (structure, list, map) normalize recursively, which is too expensive to
     * repeat for every row; their instances are shared across rows, so fingerprints are cached
     * per instance.
     *
     * @var \WeakMap<Type<mixed>, string>
     */
    private WeakMap $structuralFingerprints;

    public function __construct()
    {
        $this->structuralFingerprints = new WeakMap();
    }

    public function fits(EncoderPlan $plan, Row $row): bool
    {
        foreach ($row->entries()->all() as $entry) {
            $name = $entry->name();

            if (!array_key_exists($name, $plan->columns)) {
                return false;
            }

            $type = $entry->definition()->type();

            $fingerprint = array_key_exists($type::class, self::CONSTANT_NORMALIZE_TYPES)
                ? ($this->fingerprints[$type::class] ??= json_encode($type->normalize(), JSON_THROW_ON_ERROR))
                : ($this->structuralFingerprints[$type] ??= json_encode($type->normalize(), JSON_THROW_ON_ERROR));

            if ($fingerprint !== $plan->columns[$name]->typeFingerprint) {
                return false;
            }
        }

        return true;
    }
}
