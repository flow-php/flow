<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

use function Flow\PostgreSql\DSL\sql_format;

final class FormatSqlCommandTest extends CommandTestCase
{
    public function test_check_option_against_directory_reports_unformatted(): void
    {
        $this->fs->writeFile('dirty.sql', 'select  id   from users');
        $this->fs->writeFile('clean.sql', sql_format('SELECT id FROM users'));

        $tester = new CommandTester($this->context->command('flow.postgresql.command.sql_format'));
        $tester->execute(['--path' => $this->fs->path()->path(), '--check' => true]);

        static::assertSame(Command::FAILURE, $tester->getStatusCode());
        static::assertStringContainsString('Not formatted', $tester->getDisplay());
        static::assertStringContainsString('dirty.sql', $tester->getDisplay());
    }

    public function test_check_option_fails_for_unformatted_sql(): void
    {
        $tester = new CommandTester($this->context->command('flow.postgresql.command.sql_format'));
        $tester->execute(['sql' => 'select id,name   from users', '--check' => true]);

        static::assertSame(Command::FAILURE, $tester->getStatusCode());
        static::assertStringContainsString('not formatted', $tester->getDisplay());
    }

    public function test_check_option_succeeds_for_already_formatted_sql(): void
    {
        $tester = new CommandTester($this->context->command('flow.postgresql.command.sql_format'));
        $tester->execute(['sql' => sql_format('SELECT id, name FROM users WHERE id = 1'), '--check' => true]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('already formatted', $tester->getDisplay());
    }

    public function test_commas_at_start_of_line_option_is_applied(): void
    {
        $tester = new CommandTester($this->context->command('flow.postgresql.command.sql_format'));
        $tester->execute([
            'sql' => 'SELECT id, name, email, created_at, updated_at FROM users',
            '--commas-start-of-line' => true,
            '--max-line-length' => '20',
        ]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertMatchesRegularExpression('/\n\s*,\s*\w/', $tester->getDisplay());
    }

    public function test_fails_when_no_input_provided(): void
    {
        $tester = new CommandTester($this->context->command('flow.postgresql.command.sql_format'));
        $tester->execute([]);

        static::assertSame(Command::FAILURE, $tester->getStatusCode());
        static::assertStringContainsString(
            'Either a SQL string argument or --path option must be provided',
            $tester->getDisplay(),
        );
    }

    public function test_formats_directory_recursively(): void
    {
        $this->fs->writeFile('a.sql', 'select 1');
        $this->fs->writeFile('sub/b.sql', 'select 2');
        $this->fs->writeFile('ignored.txt', 'select 3');

        $tester = new CommandTester($this->context->command('flow.postgresql.command.sql_format'));
        $tester->execute(['--path' => $this->fs->path()->path(), '--write' => true]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('SELECT', $this->fs->readFile('a.sql'));
        static::assertStringContainsString('SELECT', $this->fs->readFile('sub/b.sql'));
        static::assertSame('select 3', $this->fs->readFile('ignored.txt'));
    }

    public function test_formats_glob_pattern(): void
    {
        $this->fs->writeFile('one.sql', 'select 1');
        $this->fs->writeFile('two.sql', 'select 2');
        $this->fs->writeFile('skip.txt', 'select 3');

        $tester = new CommandTester($this->context->command('flow.postgresql.command.sql_format'));
        $tester->execute(['--path' => $this->fs->path()->path() . '/*.sql']);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('one.sql', $tester->getDisplay());
        static::assertStringContainsString('two.sql', $tester->getDisplay());
        static::assertStringNotContainsString('skip.txt', $tester->getDisplay());
    }

    public function test_formats_single_file_to_stdout(): void
    {
        $file = $this->fs->writeFile('query.sql', 'select id,name from users where id=1');

        $tester = new CommandTester($this->context->command('flow.postgresql.command.sql_format'));
        $tester->execute(['--path' => $file->path()]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('SELECT', $tester->getDisplay());
        static::assertSame('select id,name from users where id=1', $this->fs->readFile('query.sql'));
    }

    public function test_formats_sql_string_argument(): void
    {
        $tester = new CommandTester($this->context->command('flow.postgresql.command.sql_format'));
        $tester->execute(['sql' => 'select id,name from users where id=1']);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('SELECT', $tester->getDisplay());
        static::assertStringContainsString('FROM users', $tester->getDisplay());
    }

    public function test_no_pretty_print_outputs_single_line_sql(): void
    {
        $tester = new CommandTester($this->context->command('flow.postgresql.command.sql_format'));
        $tester->execute(['sql' => 'SELECT id, name FROM users WHERE id = 1', '--no-pretty-print' => true]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringNotContainsString("\n  ", $tester->getDisplay());
    }

    public function test_reports_when_no_sql_files_found(): void
    {
        $tester = new CommandTester($this->context->command('flow.postgresql.command.sql_format'));
        $tester->execute(['--path' => $this->fs->path()->path()]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('No .sql files found', $tester->getDisplay());
    }

    public function test_writes_formatted_file_back_with_write_option(): void
    {
        $file = $this->fs->writeFile('query.sql', 'select id,name from users');

        $tester = new CommandTester($this->context->command('flow.postgresql.command.sql_format'));
        $tester->execute(['--path' => $file->path(), '--write' => true]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('Formatted:', $tester->getDisplay());
        $contents = $this->fs->readFile('query.sql');
        static::assertStringContainsString('SELECT', $contents);
        static::assertStringContainsString('FROM users', $contents);
    }
}
