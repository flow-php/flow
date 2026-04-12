<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Tests\Integration;

final class SkipOnAbstractParentIntegrationTest extends AbstractSkippedIntegrationTestCase
{
    public function test_1_abstract_parent_skip_data_persists() : void
    {
        $client = $this->client();

        $client->execute('CREATE TABLE IF NOT EXISTS _test_skip_abstract (id INT PRIMARY KEY, label TEXT)');
        $client->execute("INSERT INTO _test_skip_abstract (id, label) VALUES (1, 'abstract-skip')");

        self::assertSame(1, $client->fetchScalarInt('SELECT COUNT(*) FROM _test_skip_abstract'));
    }

    public function test_2_abstract_parent_skip_data_survived() : void
    {
        $client = $this->client();

        self::assertSame(
            1,
            $client->fetchScalarInt('SELECT COUNT(*) FROM _test_skip_abstract'),
            'Data should persist because #[SkipTransactionRollback] is on the abstract parent class',
        );

        $client->execute('DROP TABLE IF EXISTS _test_skip_abstract');
    }
}
