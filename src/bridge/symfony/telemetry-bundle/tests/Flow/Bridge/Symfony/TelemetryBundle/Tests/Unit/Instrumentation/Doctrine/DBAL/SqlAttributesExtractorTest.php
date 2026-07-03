<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Doctrine\DBAL;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\SqlAttributesExtractor;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

#[CoversClass(SqlAttributesExtractor::class)]
final class SqlAttributesExtractorTest extends TestCase
{
    #[TestWith(['SELECT * FROM users WHERE id = 1', 'SELECT', 'users'])]
    #[TestWith(['  select id from public.users', 'SELECT', 'public.users'])]
    #[TestWith(['INSERT INTO orders (id) VALUES (1)', 'INSERT', 'orders'])]
    #[TestWith(['UPDATE accounts SET active = true', 'UPDATE', 'accounts'])]
    #[TestWith(['DELETE FROM sessions WHERE id = 1', 'DELETE', 'sessions'])]
    #[TestWith(['SELECT a.* FROM a JOIN b ON a.id = b.a_id', 'SELECT', 'a'])]
    #[TestWith(['CREATE TABLE test_table (id INT)', 'CREATE', null])]
    #[TestWith(['SAVEPOINT DOCTRINE_1', 'SAVEPOINT', null])]
    #[TestWith(['not a real sql statement', null, null])]
    public function test_extract(string $sql, ?string $operation, ?string $collection): void
    {
        $attributes = (new SqlAttributesExtractor())->extract($sql);

        static::assertSame($operation, $attributes->operation);
        static::assertSame($collection, $attributes->collection);
    }
}
