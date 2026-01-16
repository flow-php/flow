<?php

declare(strict_types=1);

namespace Flow\Telemetry\Propagation;

use Flow\Telemetry\Context\Baggage;

/**
 * W3C Baggage propagator for propagating application-specific data.
 *
 * Implements the W3C Baggage specification for propagating key-value
 * pairs across service boundaries using HTTP headers.
 *
 * Header format:
 * baggage: key1=value1,key2=value2;property1;property2=propValue
 *
 * Note: Properties (metadata for baggage entries) are parsed but not preserved
 * in the current implementation as they are rarely used.
 *
 * Example usage:
 * ```php
 * $propagator = new W3CBaggage();
 *
 * // Extract from incoming request
 * $carrier = new ArrayCarrier($headers);
 * $ctx = $propagator->extract($carrier);
 * $baggage = $ctx->baggage;
 *
 * // Inject into outgoing request
 * $carrier = new ArrayCarrier();
 * $propagator->inject(new PropagationContext(baggage: $baggage), $carrier);
 * $headers = $carrier->toArray();
 * ```
 *
 * @see https://www.w3.org/TR/baggage/
 */
final readonly class W3CBaggage implements Propagator
{
    public const string HEADER_BAGGAGE = 'baggage';

    public function extract(Carrier $carrier) : PropagationContext
    {
        $baggageHeader = $carrier->get(self::HEADER_BAGGAGE);

        if ($baggageHeader === null || $baggageHeader === '') {
            return new PropagationContext(baggage: new Baggage());
        }

        $entries = [];
        $members = \explode(',', $baggageHeader);

        foreach ($members as $member) {
            $member = \trim($member);

            if ($member === '') {
                continue;
            }

            $parts = \explode(';', $member, 2);
            $keyValue = $parts[0];

            $kvParts = \explode('=', $keyValue, 2);

            if (\count($kvParts) !== 2) {
                continue;
            }

            $key = \trim($kvParts[0]);
            $value = \trim($kvParts[1]);

            if ($key === '') {
                continue;
            }

            $key = \urldecode($key);
            $value = \urldecode($value);

            $entries[$key] = $value;
        }

        return new PropagationContext(baggage: new Baggage($entries));
    }

    /**
     * @return array<string>
     */
    public function fields() : array
    {
        return [self::HEADER_BAGGAGE];
    }

    public function inject(PropagationContext $context, Carrier $carrier) : void
    {
        if ($context->baggage === null || $context->baggage->isEmpty()) {
            return;
        }

        $members = [];

        foreach ($context->baggage->all() as $key => $value) {
            $encodedKey = \urlencode($key);
            $encodedValue = \urlencode($value);

            $members[] = $encodedKey . '=' . $encodedValue;
        }

        $carrier->set(self::HEADER_BAGGAGE, \implode(',', $members));
    }
}
