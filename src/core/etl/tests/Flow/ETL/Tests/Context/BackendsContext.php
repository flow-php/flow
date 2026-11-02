<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\RustBackend;
use Generator;

use function extension_loaded;

final class BackendsContext
{
    /**
     * @return Generator<string, array{Backend}> the PHP backend, and the Rust one when flow_php is loaded
     */
    public static function backends(): Generator
    {
        yield 'php' => [new PhpBackend()];

        if (extension_loaded('flow_php')) {
            yield 'rust' => [new RustBackend()];
        }
    }

    /**
     * @param iterable<string, list<mixed>> $cases
     *
     * @return Generator<string, list<mixed>> every case once per backend, the backend first
     */
    public static function perBackend(iterable $cases): Generator
    {
        // a generator of cases is walked once per backend, so it is collected first
        $cases = [...$cases];

        foreach (self::backends() as $name => [$backend]) {
            foreach ($cases as $case => $arguments) {
                yield "{$name}: {$case}" => [$backend, ...$arguments];
            }
        }
    }
}
