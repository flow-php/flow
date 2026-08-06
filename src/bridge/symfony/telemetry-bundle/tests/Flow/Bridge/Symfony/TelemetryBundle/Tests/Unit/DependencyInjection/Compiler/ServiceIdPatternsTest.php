<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\ServiceIdPatterns;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

#[CoversClass(ServiceIdPatterns::class)]
final class ServiceIdPatternsTest extends TestCase
{
    /**
     * @param array<string> $patterns
     */
    #[TestWith([[], 'cache.system', false])]
    #[TestWith([['cache.system'], 'cache.system', true])]
    #[TestWith([['cache.system'], 'cache.validator', false])]
    #[TestWith([['/^cache\.validator.*/'], 'cache.validator.expression', true])]
    #[TestWith([['/^cache\.validator.*/'], 'cache.system', false])]
    #[TestWith([['cache.app', '/^cache\.doctrine\..*/'], 'cache.doctrine.orm.default', true])]
    #[TestWith([['cache.app', '/^cache\.doctrine\..*/'], 'cache.app', true])]
    #[TestWith([['cache.app', '/^cache\.doctrine\..*/'], 'cache.system', false])]
    public function test_matching_a_service_id_against_patterns(
        array $patterns,
        string $serviceId,
        bool $expected,
    ): void {
        static::assertSame($expected, (new ServiceIdPatterns($patterns))->matches($serviceId));
    }

    #[TestWith(['[unterminated'])]
    #[TestWith(['/unterminated'])]
    #[TestWith(['cache.system'])]
    public function test_an_invalid_regular_expression_is_compared_as_an_exact_service_id(string $pattern): void
    {
        static::assertTrue((new ServiceIdPatterns([$pattern]))->matches($pattern));
        static::assertFalse((new ServiceIdPatterns([$pattern]))->matches('some.other.service'));
    }
}
