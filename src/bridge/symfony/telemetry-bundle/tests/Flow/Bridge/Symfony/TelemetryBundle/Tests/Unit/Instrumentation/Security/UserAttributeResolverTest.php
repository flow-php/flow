<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Security;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Security\UserAttributeResolver;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Security\StaticUserAttributeProvider;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Security\TestSecurityUser;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Security\Core\Authentication\Token\NullToken;
use Symfony\Component\Security\Core\Authentication\Token\UsernamePasswordToken;

#[CoversClass(UserAttributeResolver::class)]
final class UserAttributeResolverTest extends TestCase
{
    public function test_includes_user_id_when_enabled(): void
    {
        $resolver = new UserAttributeResolver([], 'user.id', null, null, 'getEmail');

        $token = new UsernamePasswordToken(new TestSecurityUser('alice'), 'main');

        static::assertSame(['user.id' => 'alice'], $resolver->resolve($token));
    }

    public function test_omits_user_id_when_disabled(): void
    {
        $resolver = new UserAttributeResolver([], null, null, null, 'getEmail');

        $token = new UsernamePasswordToken(new TestSecurityUser('alice'), 'main');

        static::assertSame([], $resolver->resolve($token));
    }

    public function test_omits_empty_user_identifier(): void
    {
        $resolver = new UserAttributeResolver([], 'user.id', null, null, 'getEmail');

        static::assertSame([], $resolver->resolve(new NullToken()));
    }

    public function test_includes_roles_when_enabled(): void
    {
        $resolver = new UserAttributeResolver([], null, 'user.roles', null, 'getEmail');

        $token = new UsernamePasswordToken(new TestSecurityUser('alice'), 'main', ['ROLE_USER', 'ROLE_ADMIN']);

        static::assertSame(['user.roles' => ['ROLE_USER', 'ROLE_ADMIN']], $resolver->resolve($token));
    }

    public function test_omits_roles_when_empty(): void
    {
        $resolver = new UserAttributeResolver([], null, 'user.roles', null, 'getEmail');

        $token = new UsernamePasswordToken(new TestSecurityUser('alice'), 'main', []);

        static::assertSame([], $resolver->resolve($token));
    }

    public function test_includes_email_from_getter(): void
    {
        $resolver = new UserAttributeResolver([], null, null, 'user.email', 'getEmail');

        $token = new UsernamePasswordToken(new TestSecurityUser('alice', 'alice@example.com'), 'main');

        static::assertSame(['user.email' => 'alice@example.com'], $resolver->resolve($token));
    }

    public function test_omits_email_when_getter_missing(): void
    {
        $resolver = new UserAttributeResolver([], null, null, 'user.email', 'getNonExistentField');

        $token = new UsernamePasswordToken(new TestSecurityUser('alice', 'alice@example.com'), 'main');

        static::assertSame([], $resolver->resolve($token));
    }

    public function test_omits_email_when_getter_returns_non_scalar(): void
    {
        $resolver = new UserAttributeResolver([], null, null, 'user.email', 'getProfile');

        $token = new UsernamePasswordToken(new TestSecurityUser('alice', 'alice@example.com'), 'main');

        static::assertSame([], $resolver->resolve($token));
    }

    public function test_omits_email_when_value_is_null(): void
    {
        $resolver = new UserAttributeResolver([], null, null, 'user.email', 'getEmail');

        $token = new UsernamePasswordToken(new TestSecurityUser('alice'), 'main');

        static::assertSame([], $resolver->resolve($token));
    }

    public function test_uses_custom_attribute_keys(): void
    {
        $resolver = new UserAttributeResolver([], 'app.actor', 'app.actor_roles', null, 'getEmail');

        $token = new UsernamePasswordToken(new TestSecurityUser('alice'), 'main', ['ROLE_USER']);

        static::assertSame(['app.actor' => 'alice', 'app.actor_roles' => ['ROLE_USER']], $resolver->resolve($token));
    }

    public function test_merges_provider_attributes(): void
    {
        $resolver = new UserAttributeResolver(
            [new StaticUserAttributeProvider(['app.tenant_id' => 'acme', 'app.plan' => 'pro'])],
            'user.id',
            null,
            null,
            'getEmail',
        );

        $token = new UsernamePasswordToken(new TestSecurityUser('alice'), 'main');

        static::assertSame(
            ['user.id' => 'alice', 'app.tenant_id' => 'acme', 'app.plan' => 'pro'],
            $resolver->resolve($token),
        );
    }

    public function test_provider_attributes_override_builtin_on_key_collision(): void
    {
        $resolver = new UserAttributeResolver(
            [new StaticUserAttributeProvider(['user.id' => 'overridden'])],
            'user.id',
            null,
            null,
            'getEmail',
        );

        $token = new UsernamePasswordToken(new TestSecurityUser('alice'), 'main');

        static::assertSame(['user.id' => 'overridden'], $resolver->resolve($token));
    }
}
