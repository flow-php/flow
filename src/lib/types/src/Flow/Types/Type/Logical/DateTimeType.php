<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use DateInterval;
use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use DOMElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Native\String\StringTemporalParts;
use Throwable;

use function array_combine;
use function array_key_exists;
use function array_map;
use function checkdate;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_literal;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function in_array;
use function is_bool;
use function is_numeric;
use function is_string;
use function preg_match;
use function sprintf;
use function strtolower;
use function substr;

/**
 * @template T of \DateTimeInterface
 *
 * @implements Type<T>
 */
final readonly class DateTimeType implements Type
{
    public const string ISO_DATE_TIME = '/^(\d{4})-(\d{2})-(\d{2})[T ](?:[01]\d|2[0-4]):[0-5]\d(?::(?:[0-5]\d|60)(?:\.\d{1,9})?)?(Z|[+-](?:(?:[01]\d|2[0-4]):?[0-5]\d|\d{2}))?$/';

    private const array UTC_ALIASES = [
        'utc',
        'z',
        'gmt',
        'gmt0',
        'gmt+0',
        'gmt-0',
        'uct',
        'universal',
        'zulu',
        'greenwich',
        'etc/utc',
        'etc/uct',
        'etc/gmt',
        'etc/gmt0',
        'etc/gmt+0',
        'etc/gmt-0',
        'etc/universal',
        'etc/zulu',
        'etc/greenwich',
    ];

    /**
     * @var array<string, string>
     */
    private const array ABBREVIATION_LINKS = [
        'cet' => 'Europe/Brussels',
        'eet' => 'Europe/Athens',
        'est' => 'America/Panama',
        'hst' => 'Pacific/Honolulu',
        'met' => 'Europe/Brussels',
        'mst' => 'America/Phoenix',
        'wet' => 'Europe/Lisbon',
    ];

    // declared before $zone so == compares the canonical names first and never reaches the DateTimeZone objects
    private string $zoneName;

    private DateTimeZone $zone;

    public function __construct(DateTimeZone|string $zone = 'UTC')
    {
        static $utc = new DateTimeZone('UTC');
        static $identifiers = null;

        if ($zone === 'UTC') {
            $this->zoneName = 'UTC';
            $this->zone = $utc;

            return;
        }

        $identifiers ??= array_combine(
            array_map(strtolower(...), $all = DateTimeZone::listIdentifiers(DateTimeZone::ALL_WITH_BC)),
            $all,
        );
        $name = $zone instanceof DateTimeZone ? $zone->getName() : $zone;
        $key = strtolower($name);

        $canonical = match (true) {
            in_array($key, self::UTC_ALIASES, true) => 'UTC',
            array_key_exists($key, self::ABBREVIATION_LINKS) => throw new InvalidArgumentException(sprintf(
                'Time zone "%s" is parsed by PHP as a fixed-offset abbreviation without daylight-saving rules, use the region it links to: "%s"',
                $name,
                self::ABBREVIATION_LINKS[$key],
            )),
            array_key_exists($key, $identifiers) => $identifiers[$key],
            preg_match('/^[+-](?:[01]\d|2[0-3])(?::?[0-5]\d)?$/', $name) === 1 => (new DateTimeZone($name))->getName(),
            default => throw new InvalidArgumentException(sprintf(
                'Time zone "%s" cannot be a datetime column zone, use an IANA name like "Europe/Warsaw", "UTC" or an offset "+HH:MM"',
                $name,
            )),
        };

        $this->zoneName = $canonical === '+00:00' ? 'UTC' : $canonical;
        $this->zone = $this->zoneName === 'UTC' ? $utc : new DateTimeZone($this->zoneName);
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'type' => type_literal('datetime'),
            'zone' => structure_element('zone', type_string(), optional: true),
        ])->assert($data);

        return new self(array_key_exists('zone', $data) ? $data['zone'] : 'UTC');
    }

    public function assert(mixed $value): DateTimeInterface
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value): DateTimeImmutable
    {
        if ($value instanceof DateTimeImmutable) {
            return $value->getTimezone()->getName() === $this->zoneName ? $value : $value->setTimezone($this->zone);
        }

        if ($value instanceof DateTime) {
            return DateTimeImmutable::createFromMutable($value)->setTimezone($this->zone);
        }

        if ($value instanceof DOMElement) {
            $value = $value->nodeValue;
        }

        try {
            if (is_string($value)) {
                $date = [];

                if (
                    preg_match(self::ISO_DATE_TIME, $value, $date) === 1
                    && checkdate((int) $date[2], (int) $date[3], (int) $date[1])
                ) {
                    if (($date[4] ?? '') === 'Z') {
                        // timelib resolves the "Z" abbreviation by scanning its whole abbreviation table, ten
                        // times the cost of parsing the rest; the zone handed in builds the identical instant
                        static $utc = new DateTimeZone('UTC');

                        $instant = new DateTimeImmutable(substr($date[0], 0, -1), $utc);

                        return $this->zoneName === 'UTC' ? $instant : $instant->setTimezone($this->zone);
                    }

                    if (($date[4] ?? '') === '') {
                        return new DateTimeImmutable($value, $this->zone);
                    }

                    return (new DateTimeImmutable($value))->setTimezone($this->zone);
                }

                if (StringTemporalParts::isoDate($value)) {
                    return new DateTimeImmutable($value, $this->zone);
                }

                $parts = StringTemporalParts::from($value);

                if (!$parts->isDate() && !$parts->isDateTime()) {
                    // DateTimeImmutable resolves '', 'now' and '+12' against the wall clock, so the
                    // same input written twice would produce two different values
                    throw new CastingException($value, $this, reason: 'value is not a calendar date');
                }

                return (new DateTimeImmutable($value, $this->zone))->setTimezone($this->zone);
            }

            if (is_numeric($value)) {
                return (new DateTimeImmutable('@' . $value))->setTimezone($this->zone);
            }

            if (is_bool($value)) {
                return (new DateTimeImmutable('@' . (int) $value))->setTimezone($this->zone);
            }

            if ($value instanceof DateInterval) {
                return (new DateTimeImmutable('@0'))->add($value)->setTimezone($this->zone);
            }
        } catch (Throwable) {
            throw new CastingException($value, $this);
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value): bool
    {
        return $value instanceof DateTimeInterface;
    }

    /**
     * @return array{type: 'datetime', zone: string}
     */
    public function normalize(): array
    {
        return [
            'type' => 'datetime',
            'zone' => $this->zoneName,
        ];
    }

    public function toString(): string
    {
        return $this->zoneName === 'UTC' ? 'datetime' : 'datetime<' . $this->zoneName . '>';
    }

    public function zone(): DateTimeZone
    {
        return $this->zone;
    }

    public function zoneName(): string
    {
        return $this->zoneName;
    }
}
