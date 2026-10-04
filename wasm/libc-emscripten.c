/*
 * libc functions PHP calls that Emscripten leaves out of its libc build (tools/system_libs.py excludes musl's
 * src/legacy and src/process). Defined here so the link runs with -sERROR_ON_UNDEFINED_SYMBOLS=1 instead of turning
 * them into stubs that abort the runtime when called.
 */

#include <errno.h>
#include <limits.h>
#include <spawn.h>
#include <sys/resource.h>
#include <unistd.h>

/* musl's src/legacy/getdtablesize.c - php://fd/N checks N against it */
int getdtablesize(void)
{
    struct rlimit limit;

    getrlimit(RLIMIT_NOFILE, &limit);

    return limit.rlim_cur < INT_MAX ? (int) limit.rlim_cur : INT_MAX;
}

/* a browser cannot start a process: proc_open() reports ENOSYS as a PHP warning instead of aborting */
int posix_spawnp(
    pid_t *restrict pid,
    const char *restrict file,
    const posix_spawn_file_actions_t *file_actions,
    const posix_spawnattr_t *restrict attributes,
    char *const argv[restrict],
    char *const envp[restrict]
)
{
    (void) pid;
    (void) file;
    (void) file_actions;
    (void) attributes;
    (void) argv;
    (void) envp;

    return ENOSYS;
}
