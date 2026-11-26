// usage: zts_request_threads <abs zts.ini>; proves requests work on threads that did not load the extension, and a request after a parse error on the same thread gives correct results
#include <pthread.h>
#include <stdio.h>
#include <sapi/embed/php_embed.h>

#ifndef ZTS
#error This test requires a ZTS PHP embed library.
#endif

#define THREADS 8
#define REQUESTS 5

#define PARSE_ERROR \
    "(function () { try { pg_query_parse('SELEC'); return false; } " \
    "catch (\\RuntimeException $e) { return $e->getMessage() !== ''; } })()"

#define PARSE_OK \
    "pg_query_fingerprint('SELECT * FROM users WHERE id = 1') === pg_query_fingerprint('SELECT * FROM users WHERE id = 2') " \
    "&& pg_query_fingerprint('SELECT * FROM users WHERE id = 1') !== pg_query_fingerprint('SELECT * FROM orders WHERE id = 1') " \
    "&& isset(json_decode(pg_query_parse('SELECT * FROM users WHERE id = 1'), true)['stmts'][0]['stmt']['SelectStmt'])"

static int request(const char *code)
{
    int failed = 1;
    zval value;

    if (php_request_startup() == FAILURE) {
        fprintf(stderr, "php_request_startup failed\n");
        return 1;
    }

    SG(headers_sent) = 1;
    SG(request_info).no_headers = 1;
    ZVAL_UNDEF(&value);

    zend_first_try {
        if (zend_eval_string((char *) code, &value, "zts request thread") == SUCCESS && Z_TYPE(value) == IS_TRUE) {
            failed = 0;
        }
    } zend_end_try();

    zval_ptr_dtor(&value);
    php_request_shutdown(NULL);

    if (failed) {
        fprintf(stderr, "request returned false: %s\n", code);
    }

    return failed;
}

static void *serve(void *status)
{
    ts_resource(0);
    ZEND_TSRMLS_CACHE_UPDATE();

    for (int i = 0; i < REQUESTS; i++) {
        if (request(PARSE_ERROR) || request(PARSE_OK)) {
            *(int *) status = 1;
            break;
        }
    }

    ts_free_thread();

    return NULL;
}

int main(int argc, char **argv)
{
    if (argc != 2) {
        fprintf(stderr, "usage: %s /absolute/path/to/zts.ini\n", argv[0]);
        return 1;
    }

    php_embed_module.php_ini_path_override = argv[1];
    php_embed_module.php_ini_ignore_cwd = 1;

    if (php_embed_init(argc, argv) == FAILURE) {
        fprintf(stderr, "php_embed_init failed\n");
        return 1;
    }

    int status = 0;

    for (int t = 0; t < THREADS && status == 0; t++) {
        pthread_t thread;

        if (pthread_create(&thread, NULL, serve, &status) != 0 || pthread_join(thread, NULL) != 0) {
            fprintf(stderr, "thread %d failed to run\n", t);
            return 1;
        }
    }

    php_embed_shutdown();

    if (status != 0) {
        return 1;
    }

    printf("%d threads x %d rounds of 2 requests passed\n", THREADS, REQUESTS);

    return 0;
}
