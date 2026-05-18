<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache\Tests\Unit\Double;

use Symfony\Component\Cache\Marshaller\MarshallerInterface;

use function in_array;
use function serialize;
use function str_starts_with;
use function substr;
use function unserialize;

final class SpyMarshaller implements MarshallerInterface
{
    public int $marshallCalls = 0;

    /**
     * @var array<string, string>
     */
    public array $marshalled = [];

    public int $unmarshallCalls = 0;

    /**
     * @param list<string> $failKeys
     */
    public function __construct(
        private readonly array $failKeys = [],
    ) {}

    /**
     * @param array<string, mixed> $values
     * @param null|list<string> $failed
     *
     * @param-out list<string> $failed
     *
     * @return array<string, string>
     */
    public function marshall(array $values, ?array &$failed): array
    {
        $this->marshallCalls++;
        $failed = [];
        $out = [];

        foreach ($values as $key => $value) {
            if (in_array((string) $key, $this->failKeys, true)) {
                $failed[] = (string) $key;

                continue;
            }

            $encoded = 'spy:' . serialize($value);
            $this->marshalled[(string) $key] = $encoded;
            $out[$key] = $encoded;
        }

        return $out;
    }

    public function unmarshall(string $value): mixed
    {
        $this->unmarshallCalls++;

        if (!str_starts_with($value, 'spy:')) {
            return unserialize($value);
        }

        return unserialize(substr($value, 4));
    }
}
