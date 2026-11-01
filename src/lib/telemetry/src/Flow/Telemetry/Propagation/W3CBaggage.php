<?php

declare(strict_types=1);

namespace Flow\Telemetry\Propagation;

use Flow\Telemetry\Context\Baggage;

use function count;
use function explode;
use function implode;
use function trim;
use function urldecode;
use function urlencode;

/**
 * Entry properties are parsed but dropped.
 *
 * @see https://www.w3.org/TR/baggage/
 */
final readonly class W3CBaggage implements Propagator
{
    public const string HEADER_BAGGAGE = 'baggage';

    /**
     * @param Carrier<mixed> $carrier
     */
    public function extract(Carrier $carrier): PropagationContext
    {
        $baggageHeader = $carrier->get(self::HEADER_BAGGAGE);

        if ($baggageHeader === null || $baggageHeader === '') {
            return new PropagationContext(baggage: new Baggage());
        }

        $entries = [];
        $members = explode(',', $baggageHeader);

        foreach ($members as $member) {
            $member = trim($member);

            if ($member === '') {
                continue;
            }

            $parts = explode(';', $member, 2);
            $keyValue = $parts[0];

            $kvParts = explode('=', $keyValue, 2);

            if (count($kvParts) !== 2) {
                continue;
            }

            $key = trim($kvParts[0]);
            $value = trim($kvParts[1]);

            if ($key === '') {
                continue;
            }

            $key = urldecode($key);
            $value = urldecode($value);

            $entries[$key] = $value;
        }

        return new PropagationContext(baggage: new Baggage($entries));
    }

    /**
     * @return array<string>
     */
    public function fields(): array
    {
        return [self::HEADER_BAGGAGE];
    }

    /**
     * @param Carrier<mixed> $carrier
     */
    public function inject(PropagationContext $context, Carrier $carrier): void
    {
        if ($context->baggage === null || $context->baggage->isEmpty()) {
            return;
        }

        $members = [];

        foreach ($context->baggage->all() as $key => $value) {
            $encodedKey = urlencode($key);
            $encodedValue = urlencode($value);

            $members[] = $encodedKey . '=' . $encodedValue;
        }

        $carrier->set(self::HEADER_BAGGAGE, implode(',', $members));
    }
}
